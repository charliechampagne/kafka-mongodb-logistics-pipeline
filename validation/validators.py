"""
validation/validators.py
=========================
Data validation module for logistics records before MongoDB ingestion.

ARCHITECTURAL DECISION — WHY VALIDATE IN THE CONSUMER?
    In a streaming pipeline, bad data can silently corrupt your database. By validating
    at the consumer layer (just before writing to MongoDB), we create a "gate" that
    ensures only clean, well-formed records enter our data store. Records that fail
    validation are logged to a dead-letter collection for later analysis, rather than
    being silently dropped or causing downstream errors.

TRADE-OFF:
    Validating at the consumer adds latency per message (~1-2ms). For our logistics
    dataset of ~6,880 records, this is negligible. If we were processing millions of
    messages per second, we might move validation to a separate microservice or do
    lightweight checks only.

ASSUMPTIONS (DOCUMENTED FOR SHOWREEL):
    1. BookingID is required — every delivery trip must have a booking reference.
    2. vehicle_no is required — we cannot track a trip without knowing the vehicle.
    3. Origin_Location and Destination_Location are required — a trip must have endpoints.
    4. Curr_lat must be between -90 and +90 (valid latitude range).
    5. Curr_lon must be between -180 and +180 (valid longitude range).
    6. TRANSPORTATION_DISTANCE_IN_KM must be non-negative when present.
    7. trip_start_date should be a parseable date string when present.
    8. BookingID_Date should be a parseable date string when present.
    9. ontime field, when present, should be one of: 'G' (green/on-time), 'R' (red/delayed), or 'NULL'.
    10. Driver_MobileNo, when not 'NA', should contain only digits (Indian mobile numbers).
    11. Market_Regular should be either 'Market' or 'Regular' when present.
    12. GpsProvider is required — we need to know the data source for quality tracking.
    13. Duplicate BookingID records are allowed (multiple GPS pings per trip).
    14. Fields marked as "NULL" strings in CSV are treated as None/null.
    15. Empty strings are treated as missing/null values.
"""

import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

logger = logging.getLogger(__name__)


class ValidationResult:
    """
    Encapsulates the result of validating a single record.
    
    WHY A CLASS INSTEAD OF A SIMPLE BOOL?
        We need to know not just pass/fail, but WHICH validations failed and WHY.
        This is critical for debugging data quality issues and for the dead-letter
        collection where we store rejected records with their failure reasons.
    """

    def __init__(self):
        self.is_valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def add_error(self, message: str):
        """Critical failure — record should NOT be ingested."""
        self.is_valid = False
        self.errors.append(message)

    def add_warning(self, message: str):
        """Non-critical issue — record can still be ingested but flagged."""
        self.warnings.append(message)

    def to_dict(self) -> Dict:
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings
        }


def _is_null_or_empty(value: Any) -> bool:
    """
    Check if a value is effectively null/empty.
    The CSV data uses the string 'NULL', 'null', 'NA', 'na', '' as null indicators.
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip().upper() in ('NULL', 'NA', 'NONE', ''):
        return True
    return False


def _parse_date(date_str: str, field_name: str) -> Tuple[bool, str]:
    """
    Attempt to parse a date string in multiple common formats.
    
    ASSUMPTION: The logistics data uses US-style dates (M/D/YY) based on
    the sample data showing '8/17/20', '8/28/20 14:38', etc.
    """
    if _is_null_or_empty(date_str):
        return True, ""  # Null dates pass (optionality handled elsewhere)

    date_formats = [
        '%m/%d/%y',           # 8/17/20
        '%m/%d/%y %H:%M',     # 8/28/20 14:38
        '%m/%d/%Y',           # 8/17/2020
        '%m/%d/%Y %H:%M',     # 8/17/2020 14:38
        '%Y-%m-%d',           # 2020-08-17
        '%Y-%m-%d %H:%M:%S',  # 2020-08-17 14:38:00
    ]

    for fmt in date_formats:
        try:
            datetime.strptime(str(date_str).strip(), fmt)
            return True, ""
        except ValueError:
            continue

    return False, f"{field_name} has unparseable date format: '{date_str}'"


def validate_record(record: Dict[str, Any]) -> ValidationResult:
    """
    Validate a single logistics record against all business rules.

    Parameters
    ----------
    record : dict
        A deserialized Avro record from Kafka representing one delivery trip data point.

    Returns
    -------
    ValidationResult
        Object containing pass/fail status, error messages, and warnings.
    """
    result = ValidationResult()

    # =========================================================================
    # RULE 1: Required field checks (critical — records missing these are rejected)
    # =========================================================================
    required_fields = {
        'BookingID': 'Every trip must have a booking reference for traceability',
        'vehicle_no': 'Vehicle identification is mandatory for fleet tracking',
        'Origin_Location': 'Trip origin is required for route analysis',
        'Destination_Location': 'Trip destination is required for route analysis',
        'GpsProvider': 'GPS data source must be identified for quality assessment',
    }

    for field, reason in required_fields.items():
        if field not in record or _is_null_or_empty(record.get(field)):
            result.add_error(f"MISSING_REQUIRED_FIELD: '{field}' is null or missing. Reason: {reason}")

    # =========================================================================
    # RULE 2: Latitude validation (-90 to +90)
    # =========================================================================
    curr_lat = record.get('Curr_lat')
    if curr_lat is not None and not _is_null_or_empty(curr_lat):
        try:
            lat_val = float(curr_lat)
            if lat_val < -90 or lat_val > 90:
                result.add_error(f"INVALID_LATITUDE: Curr_lat={lat_val} is outside valid range [-90, 90]")
        except (ValueError, TypeError):
            result.add_error(f"INVALID_LATITUDE_TYPE: Curr_lat='{curr_lat}' is not a valid number")

    # =========================================================================
    # RULE 3: Longitude validation (-180 to +180)
    # =========================================================================
    curr_lon = record.get('Curr_lon')
    if curr_lon is not None and not _is_null_or_empty(curr_lon):
        try:
            lon_val = float(curr_lon)
            if lon_val < -180 or lon_val > 180:
                result.add_error(f"INVALID_LONGITUDE: Curr_lon={lon_val} is outside valid range [-180, 180]")
        except (ValueError, TypeError):
            result.add_error(f"INVALID_LONGITUDE_TYPE: Curr_lon='{curr_lon}' is not a valid number")

    # =========================================================================
    # RULE 4: Transportation distance must be non-negative
    # =========================================================================
    distance = record.get('TRANSPORTATION_DISTANCE_IN_KM')
    if distance is not None and not _is_null_or_empty(distance):
        try:
            dist_val = float(distance)
            if dist_val < 0:
                result.add_error(f"INVALID_DISTANCE: TRANSPORTATION_DISTANCE_IN_KM={dist_val} cannot be negative")
        except (ValueError, TypeError):
            result.add_error(f"INVALID_DISTANCE_TYPE: TRANSPORTATION_DISTANCE_IN_KM='{distance}' is not a valid number")

    # =========================================================================
    # RULE 5: Date format validation (warnings, not errors — data still usable)
    # =========================================================================
    date_fields = ['BookingID_Date', 'trip_start_date', 'trip_end_date', 'actual_eta']
    for date_field in date_fields:
        val = record.get(date_field)
        if not _is_null_or_empty(val):
            is_valid_date, date_error = _parse_date(val, date_field)
            if not is_valid_date:
                result.add_warning(date_error)

    # =========================================================================
    # RULE 6: ontime field validation
    # =========================================================================
    ontime = record.get('ontime')
    if not _is_null_or_empty(ontime):
        valid_ontime_values = {'G', 'R'}  # Green = on time, Red = delayed
        if str(ontime).strip().upper() not in valid_ontime_values:
            result.add_warning(f"UNEXPECTED_ONTIME_VALUE: ontime='{ontime}', expected 'G' or 'R'")

    # =========================================================================
    # RULE 7: Market_Regular field validation
    # =========================================================================
    market_regular = record.get('Market_Regular')
    if not _is_null_or_empty(market_regular):
        valid_market_values = {'Market', 'Regular'}
        if str(market_regular).strip() not in valid_market_values:
            result.add_warning(f"UNEXPECTED_MARKET_VALUE: Market_Regular='{market_regular}', expected 'Market' or 'Regular'")

    # =========================================================================
    # RULE 8: Driver mobile number format check (Indian mobile: 10 digits)
    # =========================================================================
    mobile = record.get('Driver_MobileNo')
    if not _is_null_or_empty(mobile):
        mobile_str = str(mobile).strip()
        if mobile_str.upper() != 'NA':
            # Remove common prefixes (+91, 91, 0)
            cleaned = re.sub(r'^(\+91|91|0)', '', mobile_str)
            if not cleaned.isdigit():
                result.add_warning(f"INVALID_MOBILE_FORMAT: Driver_MobileNo='{mobile}' contains non-numeric characters")

    # =========================================================================
    # RULE 9: Cross-field consistency — origin and destination codes
    # =========================================================================
    origin_loc = record.get('Origin_Location')
    origin_code = record.get('OriginLocation_Code')
    if not _is_null_or_empty(origin_loc) and _is_null_or_empty(origin_code):
        result.add_warning("MISSING_ORIGIN_CODE: Origin_Location is present but OriginLocation_Code is missing")

    dest_loc = record.get('Destination_Location')
    dest_code = record.get('DestinationLocation_Code')
    if not _is_null_or_empty(dest_loc) and _is_null_or_empty(dest_code):
        result.add_warning("MISSING_DEST_CODE: Destination_Location is present but DestinationLocation_Code is missing")

    # =========================================================================
    # RULE 10: Logical consistency — trip_end_date should be after trip_start_date
    # =========================================================================
    start = record.get('trip_start_date')
    end = record.get('trip_end_date')
    if not _is_null_or_empty(start) and not _is_null_or_empty(end):
        try:
            # Try parsing with common formats
            for fmt in ['%m/%d/%y %H:%M', '%m/%d/%y', '%m/%d/%Y %H:%M', '%m/%d/%Y']:
                try:
                    start_dt = datetime.strptime(str(start).strip(), fmt)
                    end_dt = datetime.strptime(str(end).strip(), fmt)
                    if end_dt < start_dt:
                        result.add_warning(
                            f"DATE_INCONSISTENCY: trip_end_date ({end}) is before trip_start_date ({start})"
                        )
                    break
                except ValueError:
                    continue
        except Exception:
            pass  # If we cannot parse, we already warned about date format above

    return result


def sanitize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize a record before MongoDB insertion.

    This function normalizes null-like values and strips whitespace from strings.
    It does NOT modify the original record — returns a new cleaned copy.

    TRADE-OFF:
        We could store raw data as-is for maximum fidelity, but normalizing
        null representations ('NULL', 'NA', '') to Python None makes downstream
        queries on MongoDB much simpler. The cost is a small loss of information
        about the original null representation, which is acceptable because the
        distinction between 'NULL' and '' carries no business meaning.
    """
    sanitized = {}
    for key, value in record.items():
        if _is_null_or_empty(value):
            sanitized[key] = None
        elif isinstance(value, str):
            sanitized[key] = value.strip()
        else:
            sanitized[key] = value
    return sanitized
