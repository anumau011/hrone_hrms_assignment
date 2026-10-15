"""
Employee Attendance & Analytics API (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (real environment variables win over a local .env)

All code lives in this one file. Sections: config, time helpers (R1-R5), models, index setup,
employee/attendance endpoints, analytics pipelines (R6-R9), explain endpoint.
"""

import json
import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from time import time_ns
from typing import Annotated, Any, Literal, Optional
 
from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import AfterValidator, BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError
 
# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
load_dotenv()  # never overrides variables that are already set in the environment
 
logger = logging.getLogger("attendance")
 
# tz_aware=True makes every datetime read from MongoDB carry UTC. Without it PyMongo returns naive
# datetimes, and converting those depends on the time zone of the machine the app runs on.
mongo_client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=3000,
)
database = mongo_client[os.getenv("MONGO_DB", "attendance_db")]
 
IST = timezone(timedelta(hours=5, minutes=30))
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
PRESENT_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]
GRACE_PERIOD_SECONDS = 10 * 60
MINIMUM_OVERTIME_MINUTES = 30
HALF_DAY_THRESHOLD_HOURS = Decimal("4.50")
MAXIMUM_SHIFT_LENGTH = timedelta(hours=24)
MAXIMUM_TREND_RANGE_DAYS = 92
 
 
# --------------------------------------------------------------------------- #
# Indexes (created at startup, safe to run more than once)
# --------------------------------------------------------------------------- #
_index_creation_lock = threading.Lock()
_indexes_created = False
 
 
def ensure_indexes() -> None:
    global _indexes_created
    with _index_creation_lock:
        if _indexes_created:
            return
        employees = database.employees
        attendance_logs = database.attendance_logs
 
        employees.create_index([("emp_code", ASCENDING)], unique=True, name="emp_code_unique")
        employees.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="department_emp_code")
        employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="department_joined_on")
        employees.create_index([("joined_on", ASCENDING)], name="joined_on")
 
        # One record per employee per day. This is what makes simultaneous punch-ins safe,
        # and it also serves every per-employee date-range read.
        attendance_logs.create_index(
            [("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="emp_code_date_unique"
        )
        # Default sort of GET /attendance, and month-range scans (leaderboard).
        attendance_logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")
        # Punch-out: "the most recent record with punch_in at or before this time".
        attendance_logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_code_punch_in")
        # Status filter combined with the default sort.
        attendance_logs.create_index(
            [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_emp_code"
        )
        _indexes_created = True
 
 
@asynccontextmanager
async def lifespan(_application: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as error:  # MongoDB not reachable yet; /health keeps retrying
        logger.warning("index creation postponed: %s", error)
    yield
 
 
app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)
 
 
@app.exception_handler(ConnectionFailure)
async def handle_database_unavailable(_request: Request, _error: ConnectionFailure):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})
 
 
# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
TIME_OF_DAY_PATTERN = r"^([01]\d|2[0-3]):[0-5]\d$"
 
 
def validate_date_string(value: str) -> str:
    # The pattern is checked first because date.fromisoformat also accepts "20260706" and "2026-W28-1".
    if not DATE_PATTERN.fullmatch(value):
        raise ValueError("must be YYYY-MM-DD")
    date.fromisoformat(value)  # rejects impossible dates such as 2026-02-30
    return value
 
 
def validate_month_string(value: str) -> str:
    if not MONTH_PATTERN.fullmatch(value):
        raise ValueError("must be YYYY-MM")
    date(int(value[:4]), int(value[5:7]), 1)  # rejects year 0000
    return value
 
 
DateString = Annotated[str, AfterValidator(validate_date_string)]
MonthString = Annotated[str, AfterValidator(validate_month_string)]
# Strict integer: floats, strings and booleans are rejected, and so are values that look like seconds.
EpochMilliseconds = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
AttendanceStatus = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
PageNumberQuery = Annotated[int, Query(ge=1)]
PageSizeQuery = Annotated[int, Query(ge=1, le=100)]
 
 
def raise_validation_error(message: str, field_name: str = "body") -> None:
    """Return a 422 from inside an endpoint, in the same shape FastAPI uses for its own validation errors."""
    raise RequestValidationError([{"type": "value_error", "loc": [field_name], "msg": message, "input": None}])
 
 
# --------------------------------------------------------------------------- #
# Time conversion helpers
# --------------------------------------------------------------------------- #
def ensure_utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
 
 
def datetime_to_epoch_milliseconds(moment: datetime) -> int:
    return (ensure_utc(moment) - UNIX_EPOCH) // timedelta(milliseconds=1)
 
 
def epoch_milliseconds_to_datetime(milliseconds: int) -> datetime:
    return UNIX_EPOCH + timedelta(milliseconds=milliseconds)
 
 
def epoch_milliseconds_to_whole_seconds(milliseconds: int) -> datetime:
    """Epoch milliseconds to a UTC datetime, truncated to whole seconds as R1 requires."""
    return UNIX_EPOCH + timedelta(seconds=milliseconds // 1000)
 
 
def truncate_to_seconds(moment: datetime) -> datetime:
    return ensure_utc(moment).replace(microsecond=0)
 
 
def current_time_in_whole_seconds() -> datetime:
    return epoch_milliseconds_to_whole_seconds(time_ns() // 1_000_000)
 
 
def current_time_in_milliseconds() -> datetime:
    return epoch_milliseconds_to_datetime(time_ns() // 1_000_000)
 
 
def parse_hours_and_minutes(time_of_day: str) -> tuple[int, int]:
    hours, minutes = time_of_day.split(":")
    return int(hours), int(minutes)
 
 
def ist_datetime_on_date(date_string: str, time_of_day: str) -> datetime:
    year, month, day = map(int, date_string.split("-"))
    hours, minutes = parse_hours_and_minutes(time_of_day)
    return datetime(year, month, day, hours, minutes, tzinfo=IST)
 
 
# --------------------------------------------------------------------------- #
# Business rules R1 - R5
# --------------------------------------------------------------------------- #
def is_overnight_shift(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM strings compare correctly as text
 
 
def calculate_attendance_date(punch_in_time: datetime, shift_start: str, shift_end: str) -> str:
    """R1: the IST calendar date of the punch-in. On an overnight shift, a punch-in earlier than
    shift_end belongs to the shift that started the day before."""
    local_punch_in = truncate_to_seconds(punch_in_time).astimezone(IST)
    attendance_day = local_punch_in.date()
    if is_overnight_shift(shift_start, shift_end):
        end_hours, end_minutes = parse_hours_and_minutes(shift_end)
        seconds_since_midnight = local_punch_in.hour * 3600 + local_punch_in.minute * 60 + local_punch_in.second
        if seconds_since_midnight < (end_hours * 60 + end_minutes) * 60:
            attendance_day -= timedelta(days=1)
    return attendance_day.isoformat()
 
 
def calculate_shift_end_time(attendance_date: str, shift_start: str, shift_end: str) -> datetime:
    shift_end_time = ist_datetime_on_date(attendance_date, shift_end)
    if is_overnight_shift(shift_start, shift_end):
        return shift_end_time + timedelta(days=1)  # an overnight shift ends the next morning
    return shift_end_time
 
 
def calculate_late_minutes(punch_in_time: datetime, shift_start: str, attendance_date: str) -> int:
    """R2: late only when strictly more than 10:00 after shift_start; then whole minutes since shift_start."""
    seconds_after_start = int(
        (truncate_to_seconds(punch_in_time) - ist_datetime_on_date(attendance_date, shift_start)).total_seconds()
    )
    return seconds_after_start // 60 if seconds_after_start > GRACE_PERIOD_SECONDS else 0
 
 
def calculate_work_hours(punch_in_time: datetime, punch_out_time: datetime) -> float:
    """R4: seconds / 3600, rounded to 2 decimals half-up with Decimal (Python's round() is not half-up)."""
    seconds_worked = int((truncate_to_seconds(punch_out_time) - truncate_to_seconds(punch_in_time)).total_seconds())
    hours_worked = Decimal(seconds_worked) / Decimal(3600)
    return float(hours_worked.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
 
 
def calculate_overtime_minutes(
    punch_out_time: datetime, shift_start: str, shift_end: str, attendance_date: str
) -> int:
    """R3: whole minutes after shift_end, counted only when there are at least 30 of them."""
    shift_end_time = calculate_shift_end_time(attendance_date, shift_start, shift_end)
    minutes_after_end = int((truncate_to_seconds(punch_out_time) - shift_end_time).total_seconds()) // 60
    return minutes_after_end if minutes_after_end >= MINIMUM_OVERTIME_MINUTES else 0
 
 
def calculate_derived_fields(
    status: str,
    punch_in_time: Optional[datetime],
    punch_out_time: Optional[datetime],
    employee: dict,
    attendance_date: str,
) -> dict:
    """The four stored, computed fields of a record (see the table in DATA_MODEL.md)."""
    if status not in PRESENT_STATUSES or punch_in_time is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
 
    late_minutes = calculate_late_minutes(punch_in_time, employee["shift_start"], attendance_date)
    if punch_out_time is None:
        return {"work_hours": None, "late_minutes": late_minutes, "overtime_minutes": 0, "half_day": False}
 
    work_hours = calculate_work_hours(punch_in_time, punch_out_time)
    overtime_minutes = calculate_overtime_minutes(
        punch_out_time, employee["shift_start"], employee["shift_end"], attendance_date
    )
    return {
        "work_hours": work_hours,
        "late_minutes": late_minutes,
        "overtime_minutes": overtime_minutes,
        "half_day": Decimal(str(work_hours)) < HALF_DAY_THRESHOLD_HOURS,  # R5 uses the rounded value
    }
 
 
# --------------------------------------------------------------------------- #
# Response formatting
# --------------------------------------------------------------------------- #
def convert_datetimes_to_epoch_milliseconds(value: Any) -> Any:
    """Walk dictionaries and lists, turning every datetime into epoch milliseconds."""
    if isinstance(value, datetime):
        return datetime_to_epoch_milliseconds(value)
    if isinstance(value, dict):
        return {key: convert_datetimes_to_epoch_milliseconds(item) for key, item in value.items()}
    if isinstance(value, list):
        return [convert_datetimes_to_epoch_milliseconds(item) for item in value]
    return value
 
 
def format_employee(employee_document: dict) -> dict:
    fields = ("emp_code", "name", "email", "department", "shift_start", "shift_end", "joined_on")
    response = {field: employee_document.get(field) for field in fields}
    created_at = employee_document.get("created_at")
    response["created_at"] = datetime_to_epoch_milliseconds(created_at) if created_at else None
    return response
 
 
def format_attendance_record(record: dict) -> dict:
    punch_in_time = record.get("punch_in")
    punch_out_time = record.get("punch_out")
    return {
        "emp_code": record["emp_code"],
        "date": record["date"],
        "status": record["status"],
        "punch_in": datetime_to_epoch_milliseconds(punch_in_time) if punch_in_time else None,
        "punch_out": datetime_to_epoch_milliseconds(punch_out_time) if punch_out_time else None,
        "work_hours": record.get("work_hours"),
        # Older records can be missing these fields: missing means 0 / false / empty.
        "late_minutes": record.get("late_minutes") or 0,
        "overtime_minutes": record.get("overtime_minutes") or 0,
        "half_day": bool(record.get("half_day", False)),
        "history": convert_datetimes_to_epoch_milliseconds(record.get("history") or []),
    }
 
 
# --------------------------------------------------------------------------- #
# Request models
# --------------------------------------------------------------------------- #
class EmployeeCreateRequest(BaseModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=TIME_OF_DAY_PATTERN)
    shift_end: str = Field("18:30", pattern=TIME_OF_DAY_PATTERN)
    joined_on: str
 
    @field_validator("joined_on")
    @classmethod
    def validate_joined_on(cls, value: str) -> str:
        return validate_date_string(value)
 
    @model_validator(mode="after")
    def validate_shift_times(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self
 
 
class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMilliseconds] = None
    status: PresenceStatus = "PRESENT"
 
 
class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMilliseconds] = None
 
 
class RegularizeRequest(BaseModel):
    status: Optional[AttendanceStatus] = None
    punch_in: Optional[EpochMilliseconds] = None
    punch_out: Optional[EpochMilliseconds] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)
    # The computed fields (work_hours and so on) are deliberately not declared, so if a client
    # sends them they are simply ignored.
 
 
# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #
@app.get("/health")
def health_check():
    try:
        mongo_client.admin.command("ping")
        ensure_indexes()  # does nothing once done; catches up if MongoDB was down at startup
    except PyMongoError:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}
 
 
# --------------------------------------------------------------------------- #
# Employees
# --------------------------------------------------------------------------- #
@app.post("/employees", status_code=201)
def create_employee(request: EmployeeCreateRequest):
    employee_document = request.model_dump()
    employee_document["created_at"] = current_time_in_milliseconds()
    try:
        database.employees.insert_one(employee_document)  # the unique index decides who wins a race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    employee_document.pop("_id", None)
    return format_employee(employee_document)
 
 
@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: PageNumberQuery = 1,
    page_size: PageSizeQuery = 20,
):
    query_filter: dict = {}
    if department is not None:
        query_filter["department"] = department
 
    total = database.employees.count_documents(query_filter)
    employee_cursor = (
        database.employees.find(query_filter, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [format_employee(employee) for employee in employee_cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}
 
 
# --------------------------------------------------------------------------- #
# Attendance: punch-in and punch-out
# --------------------------------------------------------------------------- #
def find_employee_or_404(emp_code: str) -> dict:
    employee = database.employees.find_one({"emp_code": emp_code})
    if not employee:
        raise HTTPException(404, "employee not found")
    return employee
 
 
def resolve_punch_time(punched_at: Optional[int]) -> datetime:
    if punched_at is None:
        return current_time_in_whole_seconds()
    return epoch_milliseconds_to_whole_seconds(punched_at)
 
 
@app.post("/attendance/punch-in", status_code=201)
def punch_in(request: PunchInRequest):
    employee = find_employee_or_404(request.emp_code)
    punch_in_time = resolve_punch_time(request.punched_at)
    attendance_date = calculate_attendance_date(punch_in_time, employee["shift_start"], employee["shift_end"])
 
    attendance_record = {
        "emp_code": request.emp_code,
        "date": attendance_date,
        "status": request.status,
        "punch_in": punch_in_time,
        "punch_out": None,
        **calculate_derived_fields(request.status, punch_in_time, None, employee, attendance_date),
        "history": [],
    }
    try:
        # No "check first, then insert": the unique (emp_code, date) index lets exactly one request through.
        database.attendance_logs.insert_one(attendance_record)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return format_attendance_record(attendance_record)
 
 
@app.post("/attendance/punch-out")
def punch_out(request: PunchOutRequest):
    employee = find_employee_or_404(request.emp_code)
    punch_out_time = resolve_punch_time(request.punched_at)
 
    latest_record = database.attendance_logs.find_one(
        {"emp_code": request.emp_code, "punch_in": {"$lte": punch_out_time}},
        sort=[("punch_in", DESCENDING)],
    )
    if not latest_record:
        raise HTTPException(404, "no punch-in found")
    if latest_record.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
 
    punch_in_time = truncate_to_seconds(latest_record["punch_in"])
    if punch_out_time <= punch_in_time:
        raise_validation_error("punched_at must be after punch_in", "punched_at")
    if punch_out_time - punch_in_time > MAXIMUM_SHIFT_LENGTH:
        raise_validation_error("punched_at is more than 24 hours after punch_in", "punched_at")
 
    derived_fields = calculate_derived_fields(
        latest_record["status"], punch_in_time, punch_out_time, employee, latest_record["date"]
    )
    # late_minutes was fixed at punch-in; punch-out only owns the fields from R3, R4 and R5.
    punch_out_fields = {
        "punch_out": punch_out_time,
        "work_hours": derived_fields["work_hours"],
        "overtime_minutes": derived_fields["overtime_minutes"],
        "half_day": derived_fields["half_day"],
    }
    # Compare-and-set on "still open": of two simultaneous punch-outs only one can match.
    updated_record = database.attendance_logs.find_one_and_update(
        {"_id": latest_record["_id"], "punch_out": None},
        {"$set": punch_out_fields},
        return_document=ReturnDocument.AFTER,
    )
    if updated_record is None:
        raise HTTPException(409, "already punched out")
    return format_attendance_record(updated_record)
 
 
# --------------------------------------------------------------------------- #
# Attendance: listing
# --------------------------------------------------------------------------- #
ATTENDANCE_SORT_ORDER = [("date", DESCENDING), ("emp_code", ASCENDING)]
 
 
def validate_date_range(date_from: Optional[str], date_to: Optional[str]) -> None:
    if date_from and date_to and date_from > date_to:
        raise_validation_error("date_from must not be after date_to", "date_from")
 
 
def build_attendance_filter(
    emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str], status: Optional[str]
) -> dict:
    query_filter: dict = {}
    if emp_code is not None:
        query_filter["emp_code"] = emp_code
    if date_from or date_to:
        date_condition = {}
        if date_from:
            date_condition["$gte"] = date_from
        if date_to:
            date_condition["$lte"] = date_to
        query_filter["date"] = date_condition
    if status:
        query_filter["status"] = status
    return query_filter
 
 
@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateString] = None,
    date_to: Optional[DateString] = None,
    status: Optional[AttendanceStatus] = None,
    page: PageNumberQuery = 1,
    page_size: PageSizeQuery = 20,
):
    validate_date_range(date_from, date_to)
    query_filter = build_attendance_filter(emp_code, date_from, date_to, status)
 
    if query_filter:
        total = database.attendance_logs.count_documents(query_filter)
    else:
        total = database.attendance_logs.estimated_document_count()
 
    record_cursor = (
        database.attendance_logs.find(query_filter)
        .sort(ATTENDANCE_SORT_ORDER)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [format_attendance_record(record) for record in record_cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}
 
 
# --------------------------------------------------------------------------- #
# Attendance: regularization (manual correction)
# --------------------------------------------------------------------------- #
AUDITED_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")
 
 
@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(emp_code: str, date: DateString, request: RegularizeRequest):
    # The path parameter has to be called "date" to match the contract.
    attendance_date = date
    employee = find_employee_or_404(emp_code)
    stored_record = database.attendance_logs.find_one({"emp_code": emp_code, "date": attendance_date})
    if not stored_record:
        raise HTTPException(404, "no record for that date")
 
    stored_punch_in = stored_record.get("punch_in")
    stored_punch_out = stored_record.get("punch_out")
    current_values = {
        "status": stored_record["status"],
        "punch_in": truncate_to_seconds(stored_punch_in) if stored_punch_in else None,
        "punch_out": truncate_to_seconds(stored_punch_out) if stored_punch_out else None,
        "work_hours": stored_record.get("work_hours"),
        "late_minutes": stored_record.get("late_minutes") or 0,
        "overtime_minutes": stored_record.get("overtime_minutes") or 0,
        "half_day": bool(stored_record.get("half_day", False)),
    }
 
    new_status = request.status or current_values["status"]
    requested_punch_in = (
        epoch_milliseconds_to_whole_seconds(request.punch_in) if request.punch_in is not None else None
    )
    requested_punch_out = (
        epoch_milliseconds_to_whole_seconds(request.punch_out) if request.punch_out is not None else None
    )
 
    if new_status in PRESENT_STATUSES:
        new_punch_in = requested_punch_in or current_values["punch_in"]
        new_punch_out = requested_punch_out or current_values["punch_out"]
        if new_punch_in is None:
            raise_validation_error("a presence status requires a punch_in", "punch_in")
        if requested_punch_in is not None:
            punch_in_belongs_to = calculate_attendance_date(
                new_punch_in, employee["shift_start"], employee["shift_end"]
            )
            if punch_in_belongs_to != attendance_date:
                raise_validation_error("punch_in must fall on the record's attendance date", "punch_in")
        if new_punch_out is not None:
            if new_punch_out <= new_punch_in:
                raise_validation_error("punch_out must be after punch_in", "punch_out")
            if new_punch_out - new_punch_in > MAXIMUM_SHIFT_LENGTH:
                raise_validation_error("punch_out must be within 24 hours of punch_in", "punch_out")
    else:
        # ABSENT and LEAVE clear both punch times, so sending a time with them makes no sense.
        if requested_punch_in is not None or requested_punch_out is not None:
            raise_validation_error(f"{new_status} cannot be combined with punch_in or punch_out")
        new_punch_in = None
        new_punch_out = None
 
    nothing_changed = (new_status, new_punch_in, new_punch_out) == (
        current_values["status"],
        current_values["punch_in"],
        current_values["punch_out"],
    )
    if nothing_changed:
        raise_validation_error("the request changes nothing")
 
    new_values = {
        "status": new_status,
        "punch_in": new_punch_in,
        "punch_out": new_punch_out,
        **calculate_derived_fields(new_status, new_punch_in, new_punch_out, employee, attendance_date),
    }
    changes = {
        field: {"from": current_values[field], "to": new_values[field]}
        for field in AUDITED_FIELDS
        if current_values[field] != new_values[field]
    }
    history_entry = {
        "at": current_time_in_milliseconds(),
        "by": request.regularized_by,
        "reason": request.reason,
        "changes": changes,
    }
 
    # Optimistic concurrency: the update only applies if the record is still exactly as we read it
    # (same editable fields, same number of history entries). If another correction or a punch-out
    # got there first, nothing matches and this request gets 409 instead of losing an audit entry.
    history_length = len(stored_record.get("history") or [])
    unchanged_since_read = {
        "_id": stored_record["_id"],
        "status": stored_record["status"],
        "punch_in": stored_punch_in,
        "punch_out": stored_punch_out,
        f"history.{history_length}": {"$exists": False},
    }
    if history_length:
        unchanged_since_read[f"history.{history_length - 1}"] = {"$exists": True}
 
    updated_record = database.attendance_logs.find_one_and_update(
        unchanged_since_read,
        {"$set": new_values, "$push": {"history": history_entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated_record is None:
        raise HTTPException(409, "record was modified concurrently, retry")
    return format_attendance_record(updated_record)
 
 
# --------------------------------------------------------------------------- #
# Analytics: shared building blocks
# --------------------------------------------------------------------------- #
def get_month_boundaries(month: str) -> tuple[str, str, date, date]:
    """'2026-07' -> ('2026-07-01', '2026-07-31', date(2026, 7, 1), date(2026, 7, 31))."""
    year, month_number = int(month[:4]), int(month[5:7])
    first_day = date(year, month_number, 1)
    first_day_of_next_month = date(year + (month_number == 12), month_number % 12 + 1, 1)
    last_day = first_day_of_next_month - timedelta(days=1)
    return first_day.isoformat(), last_day.isoformat(), first_day, last_day
 
 
def count_working_days(first_day: date, last_day: date, joined_on: str) -> int:
    """R7: Monday to Friday in the month, counted from joined_on for people who joined mid-month."""
    counting_from = max(first_day, date.fromisoformat(joined_on))
    if counting_from > last_day:
        return 0
    number_of_days = (last_day - counting_from).days + 1
    return sum(
        1 for day_offset in range(number_of_days) if (counting_from + timedelta(days=day_offset)).weekday() < 5
    )
 
 
def round_half_up(value: Decimal, decimal_places: str) -> float:
    return float(value.quantize(Decimal(decimal_places), rounding=ROUND_HALF_UP))
 
 
def is_weekday_expression(date_field: str) -> dict:
    """True when the YYYY-MM-DD string in date_field is Monday to Friday.
    Dates are plain calendar dates, so evaluating them in UTC is correct."""
    return {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": date_field}}}, 5]}
 
 
def is_present_expression(status_field: str) -> dict:
    return {"$in": [status_field, PRESENT_STATUSES]}
 
 
def present_half_days_expression(
    status_field: str, half_day_field: str, weekday_date_field: Optional[str] = None
) -> dict:
    """How much a record counts towards presence, in half days: 2 = full day, 1 = half day, 0 = not present.
    Counting in half days keeps every sum an exact integer (R5, R6, and R7 when weekday_date_field is given)."""
    conditions = [is_present_expression(status_field)]
    if weekday_date_field:
        conditions.append(is_weekday_expression(weekday_date_field))
    return {
        "$cond": [
            {"$and": conditions},
            {"$cond": [{"$eq": [{"$ifNull": [half_day_field, False]}, True]}, 1, 2]},
            0,
        ]
    }
 
 
def count_if_late_expression(late_minutes_field: str) -> dict:
    return {"$cond": [{"$gt": [{"$ifNull": [late_minutes_field, 0]}, 0]}, 1, 0]}
 
 
def count_if_status_expression(status_field: str, status: str) -> dict:
    return {"$cond": [{"$eq": [status_field, status]}, 1, 0]}
 
 
def run_aggregation(collection_name: str, pipeline: list) -> list:
    return list(database[collection_name].aggregate(pipeline, allowDiskUse=True))
 
 
# --------------------------------------------------------------------------- #
# Analytics: pipelines
# --------------------------------------------------------------------------- #
def build_employee_monthly_pipeline(emp_code: str, month: str) -> tuple[str, list]:
    month_start, month_end, _, _ = get_month_boundaries(month)
    return "attendance_logs", [
        {"$match": {"emp_code": emp_code, "date": {"$gte": month_start, "$lte": month_end}}},
        {
            "$group": {
                "_id": None,
                "present_half_days": {"$sum": present_half_days_expression("$status", "$half_day", "$date")},
                "leave_days": {"$sum": count_if_status_expression("$status", "LEAVE")},
                "late_count": {"$sum": count_if_late_expression("$late_minutes")},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]
 
 
def build_department_summary_pipeline(month: str, department: Optional[str]) -> tuple[str, list]:
    month_start, month_end, _, _ = get_month_boundaries(month)
    has_work_hours = {"$and": [is_present_expression("$logs.status"), {"$isNumber": "$logs.work_hours"}]}
 
    employee_filter: dict = {"joined_on": {"$lte": month_end}}  # R9: joined by the end of the month
    if department is not None:
        employee_filter["department"] = department
 
    return "employees", [
        {"$match": employee_filter},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": month_start, "$lte": month_end}}},
                    {
                        "$project": {
                            "_id": 0, "date": 1, "status": 1, "half_day": 1, "work_hours": 1, "late_minutes": 1,
                        }
                    },
                ],
                "as": "logs",
            }
        },
        # preserveNullAndEmptyArrays keeps employees with no logs (they still count in headcount).
        # log_index is null for them and 0 for everybody else's first log, so each person counts once.
        {"$unwind": {"path": "$logs", "includeArrayIndex": "log_index", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": {"$cond": [{"$in": ["$log_index", [0, None]]}, 1, 0]}},
                "present_half_days": {
                    "$sum": present_half_days_expression("$logs.status", "$logs.half_day", "$logs.date")
                },
                # Stored work_hours have 2 decimals, so summing them in hundredths keeps the total exact.
                # The average is then rounded half-up in Python.
                "work_hours_in_hundredths": {
                    "$sum": {"$cond": [has_work_hours, {"$round": [{"$multiply": ["$logs.work_hours", 100]}, 0]}, 0]}
                },
                "records_with_work_hours": {"$sum": {"$cond": [has_work_hours, 1, 0]}},
                "late_count": {"$sum": count_if_late_expression("$logs.late_minutes")},
                "total_late_minutes": {"$sum": {"$ifNull": ["$logs.late_minutes", 0]}},
                "leave_count": {"$sum": count_if_status_expression("$logs.status", "LEAVE")},
                "on_duty_count": {"$sum": count_if_status_expression("$logs.status", "ON_DUTY")},
            }
        },
        {"$match": {"headcount": {"$gt": 0}}},
        {"$sort": {"_id": 1}},
    ]
 
 
def build_late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> tuple[str, list]:
    month_start, month_end, _, _ = get_month_boundaries(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": month_start, "$lte": month_end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "employee"}},
        {"$unwind": "$employee"},  # logs whose employee does not exist are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"employee.department": department}})  # rank inside the department only
 
    pipeline += [
        # $rank is standard competition ranking: ties share a rank and the next one is skipped (1, 2, 2, 4).
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit is applied after ranking, so ties at the cutoff stay
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$employee.name",
                "department": "$employee.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return "attendance_logs", pipeline
 
 
def build_department_trend_pipeline(department: str, from_date: str, to_date: str) -> tuple[str, list]:
    number_of_days = (date.fromisoformat(to_date) - date.fromisoformat(from_date)).days + 1
    day_as_string = {"$dateToString": {"format": "%Y-%m-%d", "date": "$$day.day_date"}}
 
    # Step 1: one element per calendar day in the range, built with $range so days without any logs exist too.
    every_day_in_range = {
        "$map": {
            "input": {"$range": [0, number_of_days]},
            "as": "day_offset",
            "in": {
                "day_date": {
                    "$dateAdd": {
                        "startDate": {"$dateFromString": {"dateString": from_date}},
                        "unit": "day",
                        "amount": "$$day_offset",
                    }
                }
            },
        }
    }
 
    # Step 2: for each day, the headcount (R9) and that day's totals from the lookup (if any).
    days_with_totals = {
        "$map": {
            "input": "$days",
            "as": "day",
            "in": {
                "$let": {
                    "vars": {"day_string": day_as_string},
                    "in": {
                        "date": "$$day_string",
                        "is_working_day": {"$lte": [{"$isoDayOfWeek": "$$day.day_date"}, 5]},
                        "headcount": {
                            "$size": {
                                "$filter": {
                                    "input": "$joined_on_dates",
                                    "as": "joined_on_date",
                                    "cond": {"$lte": ["$$joined_on_date", "$$day_string"]},
                                }
                            }
                        },
                        "daily_total": {
                            "$arrayElemAt": [
                                {
                                    "$filter": {
                                        "input": "$daily_totals",
                                        "as": "daily_total",
                                        "cond": {"$eq": ["$$daily_total._id", "$$day_string"]},
                                    }
                                },
                                0,
                            ]
                        },
                    },
                }
            },
        }
    }
 
    # Step 3: the attendance rate, in ten-thousandths, rounded half-up using only integer arithmetic:
    # floor(present / headcount * 10000 + 1/2) = floor((10000 * half_days + headcount) / (2 * headcount))
    present_half_days = {"$ifNull": ["$$day.daily_total.present_half_days", 0]}
    days_with_rates = {
        "$map": {
            "input": "$days",
            "as": "day",
            "in": {
                "date": "$$day.date",
                "is_working_day": "$$day.is_working_day",
                "headcount": "$$day.headcount",
                "present_half_days": present_half_days,
                "late_count": {"$ifNull": ["$$day.daily_total.late_count", 0]},
                "rate_in_ten_thousandths": {
                    "$cond": [
                        {"$and": ["$$day.is_working_day", {"$gt": ["$$day.headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": [10000, present_half_days]}, "$$day.headcount"]},
                                    {"$multiply": [2, "$$day.headcount"]},
                                ]
                            }
                        },
                        None,
                    ]
                },
            },
        }
    }
 
    # Step 4: 7-day moving average over the current row and up to 6 rows before it, inside the range only,
    # ignoring null rates. Also rounded half-up in ten-thousandths.
    rates_in_window = {
        "$filter": {
            "input": {
                "$map": {
                    "input": {
                        "$slice": [
                            "$days",
                            "$$window_start",
                            {"$add": [{"$subtract": ["$$day_index", "$$window_start"]}, 1]},
                        ]
                    },
                    "as": "window_day",
                    "in": "$$window_day.rate_in_ten_thousandths",
                }
            },
            "as": "rate",
            "cond": {"$ne": ["$$rate", None]},
        }
    }
    moving_average = {
        "$cond": [
            {"$eq": [{"$size": "$$window_rates"}, 0]},
            None,
            {
                "$floor": {
                    "$divide": [
                        {"$add": [{"$multiply": [2, {"$sum": "$$window_rates"}]}, {"$size": "$$window_rates"}]},
                        {"$multiply": [2, {"$size": "$$window_rates"}]},
                    ]
                }
            },
        ]
    }
    days_with_moving_average = {
        "$map": {
            "input": {"$range": [0, {"$size": "$days"}]},
            "as": "day_index",
            "in": {
                "$let": {
                    "vars": {"window_start": {"$max": [0, {"$subtract": ["$$day_index", 6]}]}},
                    "in": {
                        "$let": {
                            "vars": {"window_rates": rates_in_window},
                            "in": {
                                "$mergeObjects": [
                                    {"$arrayElemAt": ["$days", "$$day_index"]},
                                    {"moving_average_in_ten_thousandths": moving_average},
                                ]
                            },
                        }
                    },
                }
            },
        }
    }
 
    return "employees", [
        {"$match": {"department": department}},
        {
            "$group": {
                "_id": None,
                "employee_codes": {"$push": "$emp_code"},
                "joined_on_dates": {"$push": "$joined_on"},
            }
        },
        # Per-day totals of this department's logs (an array localField matches any of the codes).
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "employee_codes",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": from_date, "$lte": to_date}}},
                    {
                        "$group": {
                            "_id": "$date",
                            # present_count counts any day, weekend included, so no weekday filter here
                            "present_half_days": {"$sum": present_half_days_expression("$status", "$half_day")},
                            "late_count": {"$sum": count_if_late_expression("$late_minutes")},
                        }
                    },
                ],
                "as": "daily_totals",
            }
        },
        {"$set": {"days": every_day_in_range}},
        {"$set": {"days": days_with_totals}},
        {"$set": {"days": days_with_rates}},
        {"$set": {"days": days_with_moving_average}},
        {"$project": {"_id": 0, "days": 1}},
        {"$unwind": "$days"},
        {"$replaceRoot": {"newRoot": "$days"}},
    ]
 
 
# --------------------------------------------------------------------------- #
# Analytics endpoints
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly_summary(emp_code: str, month: MonthString):
    employee = database.employees.find_one({"emp_code": emp_code}, {"joined_on": 1})
    if not employee:
        raise HTTPException(404, "employee not found")
 
    _, _, first_day, last_day = get_month_boundaries(month)
    working_days = count_working_days(first_day, last_day, employee["joined_on"])
 
    collection_name, pipeline = build_employee_monthly_pipeline(emp_code, month)
    results = run_aggregation(collection_name, pipeline)
    totals = results[0] if results else {}
    present_days = Decimal(int(totals.get("present_half_days", 0))) / 2
 
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": float(present_days),
        "leave_days": int(totals.get("leave_days", 0)),
        "late_count": int(totals.get("late_count", 0)),
        "total_late_minutes": int(totals.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(totals.get("total_overtime_minutes", 0)),
        "attendance_pct": round_half_up(present_days * 100 / working_days, "0.01") if working_days else None,
    }
 
 
@app.get("/analytics/departments/summary")
def department_summary(month: MonthString, department: Optional[str] = None):
    collection_name, pipeline = build_department_summary_pipeline(month, department)
    items = []
    for row in run_aggregation(collection_name, pipeline):
        records_with_work_hours = int(row["records_with_work_hours"])
        if records_with_work_hours:
            average_hours = Decimal(int(row["work_hours_in_hundredths"])) / records_with_work_hours / 100
            average_work_hours = round_half_up(average_hours, "0.01")
        else:
            average_work_hours = None
        items.append(
            {
                "department": row["_id"],
                "headcount": int(row["headcount"]),
                "present_days": float(Decimal(int(row["present_half_days"])) / 2),
                "avg_work_hours": average_work_hours,
                "late_count": int(row["late_count"]),
                "total_late_minutes": int(row["total_late_minutes"]),
                "leave_count": int(row["leave_count"]),
                "on_duty_count": int(row["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}
 
 
@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: MonthString,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    department: Optional[str] = None,
):
    collection_name, pipeline = build_late_leaderboard_pipeline(month, limit, department)
    items = [
        {
            "rank": int(row["rank"]),
            "emp_code": row["emp_code"],
            "name": row["name"],
            "department": row["department"],
            "total_late_minutes": int(row["total_late_minutes"]),
            "late_count": int(row["late_count"]),
        }
        for row in run_aggregation(collection_name, pipeline)
    ]
    return {"month": month, "items": items}
 
 
def validate_trend_range(from_date: str, to_date: str) -> None:
    start_date, end_date = date.fromisoformat(from_date), date.fromisoformat(to_date)
    if end_date < start_date:
        raise_validation_error("to must not be before from", "to")
    if (end_date - start_date).days + 1 > MAXIMUM_TREND_RANGE_DAYS:
        raise_validation_error(f"range must not exceed {MAXIMUM_TREND_RANGE_DAYS} days", "to")
 
 
def ten_thousandths_to_rate(value: Optional[float]) -> Optional[float]:
    return None if value is None else int(value) / 10000
 
 
@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_date: Annotated[DateString, Query(alias="from")],
    to_date: Annotated[DateString, Query(alias="to")],
):
    validate_trend_range(from_date, to_date)
    if database.employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
 
    collection_name, pipeline = build_department_trend_pipeline(department, from_date, to_date)
    items = [
        {
            "date": row["date"],
            "is_working_day": bool(row["is_working_day"]),
            "headcount": int(row["headcount"]),
            "present_count": float(Decimal(int(row["present_half_days"])) / 2),
            "late_count": int(row["late_count"]),
            "attendance_rate": ten_thousandths_to_rate(row["rate_in_ten_thousandths"]),
            "moving_avg_7d": ten_thousandths_to_rate(row["moving_average_in_ten_thousandths"]),
        }
        for row in run_aggregation(collection_name, pipeline)
    ]
    return {"department": department, "items": items}
 
 
# --------------------------------------------------------------------------- #
# Explain
# --------------------------------------------------------------------------- #
ExplainableEndpoint = Literal[
    "attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"
]
 
 
def require_query_parameters(**parameters: Any) -> None:
    for parameter_name, value in parameters.items():
        if value is None:
            raise_validation_error(f"query parameter '{parameter_name}' is required", parameter_name)
 
 
@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainableEndpoint,
    emp_code: Optional[str] = None,
    month: Optional[MonthString] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[DateString] = None,
    date_to: Optional[DateString] = None,
    status: Optional[AttendanceStatus] = None,
    from_date: Annotated[Optional[DateString], Query(alias="from")] = None,
    to_date: Annotated[Optional[DateString], Query(alias="to")] = None,
    page: PageNumberQuery = 1,
    page_size: PageSizeQuery = 20,
):
    # Each branch builds the query exactly the way the real endpoint does.
    if endpoint == "attendance_list":
        validate_date_range(date_from, date_to)
        collection_name = "attendance_logs"
        explained_command: dict = {
            "find": collection_name,
            "filter": build_attendance_filter(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT_ORDER),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
    else:
        if endpoint == "employee_monthly":
            require_query_parameters(emp_code=emp_code, month=month)
            collection_name, pipeline = build_employee_monthly_pipeline(emp_code, month)
        elif endpoint == "department_summary":
            require_query_parameters(month=month)
            collection_name, pipeline = build_department_summary_pipeline(month, department)
        elif endpoint == "late_leaderboard":
            require_query_parameters(month=month)
            collection_name, pipeline = build_late_leaderboard_pipeline(month, limit, department)
        else:  # department_trend
            require_query_parameters(department=department, **{"from": from_date, "to": to_date})
            validate_trend_range(from_date, to_date)
            collection_name, pipeline = build_department_trend_pipeline(department, from_date, to_date)
        explained_command = {"aggregate": collection_name, "pipeline": pipeline, "cursor": {}}
 
    explain_output = database.command({"explain": explained_command, "verbosity": "executionStats"})
    return {
        "endpoint": endpoint,
        "collection": collection_name,
        "explain": json.loads(json_util.dumps(explain_output, json_options=json_util.RELAXED_JSON_OPTIONS)),
    }
 
 
# --------------------------------------------------------------------------- #
# OpenAPI spec and Swagger UI (convenience pages for trying the API by hand)
# --------------------------------------------------------------------------- #
OPENAPI_SPEC_PATH = Path.cwd() / "openapi.yaml"
 
 
@app.get("/openapi.yaml")
def serve_openapi_specification():
    return FileResponse(OPENAPI_SPEC_PATH, media_type="application/yaml")
 
 
@app.get("/swagger", response_class=HTMLResponse)
def serve_swagger_page():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>API Documentation</title>
        <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
    </head>
    <body>
        <div id="swagger-ui"></div>
        <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
        <script>
            SwaggerUIBundle({
                url: "/openapi.yaml",
                dom_id: "#swagger-ui",
                deepLinking: true
            });
        </script>
    </body>
    </html>
    """
 