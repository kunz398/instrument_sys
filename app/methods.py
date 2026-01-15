import json
import re
from typing import List, Any
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import AsyncSessionLocal
from app.models import Token
import logging
import math
logger = logging.getLogger(__name__)
def calculate_sea_level_mean(slevel, mean):
    return float(slevel) - abs(float(mean))

def apply_intervals(data: List[dict], interval: int) -> List[dict]:
    if not data or interval <= 0:
        return data
    return [data[i] for i in range(0, len(data), interval + 1)]

def filter_bad_data(data: List[dict], bad_data_str: str) -> List[dict]:
    """
    Filter out bad data values and replace them with -999.
      - "<X": any numeric value less than X is replaced
      - ">X": any numeric value greater than X is replaced
      - "V": any value equal to V is replaced (string or numeric)
    """
    if not bad_data_str or not data:
        return data

    # Parse rules
    tokens = [t.strip() for t in bad_data_str.split(',') if t.strip()]
    if not tokens:
        return data

    rules = []  # tuples of (kind, ...): ('cmp','lt'|'gt', float) | ('eq_num', float) | ('eq_str', str)
    for tok in tokens:
        if tok and tok[0] in ('<', '>') and len(tok) > 1:
            op = 'lt' if tok[0] == '<' else 'gt'
            try:
                thr = float(tok[1:].strip())
                rules.append(('cmp', op, thr))
            except Exception:
                # Ignore malformed threshold; continue with next token
                continue
        else:
            # equality rule; try numeric first
            try:
                num = float(tok)
                rules.append(('eq_num', num))
            except Exception:
                rules.append(('eq_str', tok))

    if not rules:
        return data

    # Time-related fields to skip
    time_fields = {'time', 'timestamp', 'obs_time_utc', 'stime', 'date', 'datetime'}

    def is_bad_value(val: Any) -> bool:
        # lazily compute numeric/string representations
        num_val = None
        num_ready = False
        str_val = None
        for r in rules:
            kind = r[0]
            if kind == 'cmp':
                if not num_ready:
                    try:
                        num_val = float(val)
                    except Exception:
                        num_val = None
                    num_ready = True
                if num_val is None:
                    continue
                _, op, thr = r
                if op == 'lt' and num_val < thr:
                    return True
                if op == 'gt' and num_val > thr:
                    return True
            elif kind == 'eq_num':
                if not num_ready:
                    try:
                        num_val = float(val)
                    except Exception:
                        num_val = None
                    num_ready = True
                if num_val is not None and num_val == r[1]:
                    return True
            elif kind == 'eq_str':
                if str_val is None:
                    str_val = str(val)
                if str_val == r[1]:
                    return True
        return False

    filtered_data = []
    for entry in data:
        filtered_entry = {}
        for key, value in entry.items():
            # Skip time-related fields
            if key.lower() in time_fields:
                filtered_entry[key] = value
            else:
                filtered_entry[key] = -999 if is_bad_value(value) else value
        filtered_data.append(filtered_entry)

    return filtered_data

def to_iso_z(ts: Any) -> str:
    """
    Normalize various timestamp formats to ISO 8601 UTC string (YYYY-MM-DDTHH:MM:SSZ).
    Handles:
      - ISO strings with or without timezone or milliseconds
      - Microsoft JSON date format: /Date(1730592000000+1300)/
    Returns original string on failure.
    """
    if ts is None:
        return None
    s = str(ts).strip()
    if not s:
        return s
    try:
        # Handle Microsoft JSON date format
        ms_match = re.match(r"^/Date\((\d+)([+-]\d{4})?\)/$", s)
        if ms_match:
            ms = int(ms_match.group(1))
            dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        # Handle standard ISO formats
        # Normalize trailing Z for fromisoformat
        if s.endswith('Z'):
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(s)
            # If naive, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        # Fallback: return as-is if parsing fails
        return s

async def spot_method(station, limit=100, start: str = None, end: str = None) -> List[dict]:
    """
    Fetch data from station's source_url, sort by timestamp, and transform column names
    """
    try:
        source_url = station.source_url
        variable_id = station.variable_id or ''
        variable_label = station.variable_label or ''
        
        # Replace token placeholder if station has a token_id
        if station.token_id and "REPALCE_TOKEN_STRING" in source_url:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(Token).where(Token.id == station.token_id))
                token_obj = result.scalar_one_or_none()
                if token_obj:
                    source_url = source_url.replace("REPALCE_TOKEN_STRING", token_obj.token)
        
        id_columns = [col.strip() for col in variable_id.split(',') if col.strip()]
        label_columns = [col.strip() for col in variable_label.split(',') if col.strip()]
        if len(id_columns) != len(label_columns):
            return []
        column_mapping = dict(zip(id_columns, label_columns))
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(source_url)
            response.raise_for_status()
            data = response.json()
        waves_data = data.get('data', {}).get('waves', [])
        if not waves_data:
            return []
        def parse_timestamp(timestamp_str):
            try:
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except:
                return datetime.min
        sorted_waves = sorted(
            waves_data,
            key=lambda x: parse_timestamp(x.get('timestamp', '')),
            reverse=True
        )
        transformed_waves = []
        for wave in sorted_waves:
            transformed_wave = {}
            for old_key, new_key in column_mapping.items():
                if old_key in wave:
                    transformed_wave[new_key] = wave[old_key]
            transformed_waves.append(transformed_wave)
        interval = int(getattr(station, 'intervals', 0) or 0)
        filtered_data = filter_bad_data(transformed_waves[:limit], getattr(station, 'bad_data', None))
        # Datetime filter
        if start or end:
            def in_range(entry):
                t = entry.get('time') or entry.get('timestamp') or entry.get('obs_time_utc') or entry.get('stime') or entry.get('date') or entry.get('datetime')
                if not t:
                    return False
                try:
                    dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                except:
                    return False
                if start:
                    try:
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        if dt < start_dt:
                            return False
                    except:
                        pass
                if end:
                    try:
                        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        if dt > end_dt:
                            return False
                    except:
                        pass
                return True
            filtered_data = [d for d in filtered_data if in_range(d)]
        return apply_intervals(filtered_data, interval)
    except Exception as e:
        return []

async def pacioos_method(station, limit=100, start: str = None, end: str = None) -> List[dict]:
    """
    Parse GeoJSON wave data, map field names, and return sorted wave entries.
    """
    try:
        source_url = station.source_url
        variable_id = station.variable_id or ''
        variable_label = station.variable_label or ''
        now = datetime.now()
        end_time = now.isoformat(timespec='seconds') + "Z"
        start_time = (now - timedelta(days=7)).isoformat(timespec='seconds') + "Z"
        source_url = station.source_url.replace("START_TIME", start_time)
        source_url = source_url.replace("END_TIME", end_time)
        source_url = source_url.replace("STATION_ID", station.station_id)
        # return f"start:{start_time} \n end:{end_time} \n url:{source_url}"
        id_columns = [col.strip() for col in variable_id.split(',') if col.strip()]
        label_columns = [col.strip() for col in variable_label.split(',') if col.strip()]
        if len(id_columns) != len(label_columns):
            return []
        column_mapping = dict(zip(id_columns, label_columns))
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(source_url)
            response.raise_for_status()
            data = response.json()
        features = data.get("features", [])
        if not features:
            return []
        def parse_timestamp(ts: str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except:
                return datetime.min
        transformed_waves = []
        for feature in features:
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])
            if not props or len(coords) != 2:
                continue
            transformed_wave = {}
            for old_key, new_key in column_mapping.items():
                if old_key in props:
                    transformed_wave[new_key] = props[old_key]
            transformed_wave["lon_deg"] = coords[0]
            transformed_wave["lat_deg"] = coords[1]
            transformed_waves.append(transformed_wave)
        sorted_waves = sorted(
            transformed_waves,
            key=lambda x: parse_timestamp(x.get("obs_time_utc", "")),
            reverse=True
        )
        interval = int(getattr(station, 'intervals', 0) or 0)
        filtered_data = filter_bad_data(sorted_waves[:limit], getattr(station, 'bad_data', None))
        # Datetime filter
        if start or end:
            def in_range(entry):
                t = entry.get('time') or entry.get('timestamp') or entry.get('obs_time_utc') or entry.get('stime') or entry.get('date') or entry.get('datetime')
                if not t:
                    return False
                try:
                    dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                except:
                    return False
                if start:
                    try:
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        if dt < start_dt:
                            return False
                    except:
                        pass
                if end:
                    try:
                        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        if dt > end_dt:
                            return False
                    except:
                        pass
                return True
            filtered_data = [d for d in filtered_data if in_range(d)]
        return apply_intervals(filtered_data, interval)
    except Exception as e:
        return []



async def dart_method(station, limit=100, start: str = None, end: str = None) -> List[dict]:
    """
    Parse NDBC DART data from text format, map field names, and return sorted entries.
    """
    try:
        source_url = station.source_url
        variable_id = station.variable_id or ''
        variable_label = station.variable_label or ''
        
        # Calculate date range (7 days ago to today)
        now = datetime.now()
        end_date = now
        start_date = now - timedelta(days=7)
        
        # Replace placeholders in the URL
        source_url = source_url.replace("STATION_ID", station.station_id)
        source_url = source_url.replace("START_MONTH", str(start_date.month))
        source_url = source_url.replace("START_DAY", str(start_date.day))
        source_url = source_url.replace("START_YEAR", str(start_date.year))
        source_url = source_url.replace("END_MONTH", str(end_date.month))
        source_url = source_url.replace("END_DAY", str(end_date.day))
        source_url = source_url.replace("END_YEAR", str(end_date.year))
        
        # Parse column mappings
        id_columns = [col.strip() for col in variable_id.split(',') if col.strip()]
        label_columns = [col.strip() for col in variable_label.split(',') if col.strip()]
        if len(id_columns) != len(label_columns):
            return []
        column_mapping = dict(zip(id_columns, label_columns))
        
        # Fetch data
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(source_url)
            response.raise_for_status()
            data_text = response.text
        
        # Parse the text response
        lines = data_text.strip().split('\n')
        transformed_data = []
        
        for line in lines:
            # Skip header lines and empty lines
            if line.startswith('#') or not line.strip():
                continue
            
            # Parse data line: YY MM DD hh mm ss T HEIGHT
            parts = line.strip().split()
            if len(parts) >= 8:
                try:
                    year = int(parts[0])
                    month = int(parts[1])
                    day = int(parts[2])
                    hour = int(parts[3])
                    minute = int(parts[4])
                    second = int(parts[5])
                    height = float(parts[7])
                    
                    # Create datetime object
                    dt = datetime(year, month, day, hour, minute, second)
                    
                    
                    station_mean = station.mean if hasattr(station, 'mean') else 0
                    if station_mean != 0:
                        height = calculate_sea_level_mean(height, station_mean)
                        # print(f"mean: {height}")
                    data_entry = {
                        'time': dt.isoformat(timespec='seconds') + "Z",
                        'm': height,
                        'lon_deg': station.longitude,
                        'lat_deg': station.latitude
                    }
                    
                    # Transform to label names
                    transformed_entry = {}
                    for old_key, new_key in column_mapping.items():
                        if old_key in data_entry:
                            transformed_entry[new_key] = data_entry[old_key]
                    
                    transformed_data.append(transformed_entry)
                    
                except (ValueError, IndexError):
                    continue
        
        # Sort by date/time (newest first)
        def parse_timestamp(entry):
            try:
                timestamp_str = entry.get('time', '') or entry.get('obs_time_utc', '')
                if timestamp_str:
                    # Handle different timestamp formats
                    if 'Z' in timestamp_str:
                        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    elif '+' in timestamp_str:
                        return datetime.fromisoformat(timestamp_str)
                    else:
                        # Try parsing as ISO format without timezone
                        return datetime.fromisoformat(timestamp_str)
                return datetime.min
            except:
                return datetime.min
        
        sorted_data = sorted(
            transformed_data,
            key=lambda x: parse_timestamp(x),
            reverse=True
        )
        
        interval = int(getattr(station, 'intervals', 0) or 0)
        filtered_data = filter_bad_data(sorted_data[:limit], getattr(station, 'bad_data', None))
        # Datetime filter
        if start or end:
            def in_range(entry):
                t = entry.get('time') or entry.get('timestamp') or entry.get('obs_time_utc') or entry.get('stime') or entry.get('date') or entry.get('datetime')
                if not t:
                    return False
                try:
                    dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                except:
                    return False
                if start:
                    try:
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        if dt < start_dt:
                            return False
                    except:
                        pass
                if end:
                    try:
                        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        if dt > end_dt:
                            return False
                    except:
                        pass
                return True
            filtered_data = [d for d in filtered_data if in_range(d)]
        return apply_intervals(filtered_data, interval)
    
    except Exception as e:
        return []


async def ioc_method(station, limit=100, start: str = None, end: str = None) -> List[dict]:
    """
    Parse IOC sea level monitoring data, map field names, and return sorted entries.
    """
    try:
        # # print("=== IOC METHOD START ===")
        source_url = station.source_url
        variable_id = station.variable_id or ''
        variable_label = station.variable_label or ''
        # # print(f"Source URL: {source_url}")
        # # print(f"Variable ID: {variable_id}")
        # # print(f"Variable Label: {variable_label}")
        
        # Calculate date range (7 days ago to today)
        now = datetime.now()
        end_time = now.isoformat(timespec='seconds') + "Z"
        start_time = (now - timedelta(days=7)).isoformat(timespec='seconds') + "Z"
        
        # Replace placeholders in the URL
        source_url = source_url.replace("STATION_ID", station.station_id)
        source_url = source_url.replace("TIME_START", start_time)
        source_url = source_url.replace("TIME_END", end_time)
        source_url = source_url.replace("LIMIT_DATA", str(limit))
        
        # # print(f"Station ID: {station.station_id}")
        # # print(f"Start time: {start_time}")
        # # print(f"End time: {end_time}")
        # print(f"Final URL: {source_url}")
        
        # Get API key from token if station has a token_id
        api_key = None
       
        if station.token_id:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(Token).where(Token.id == station.token_id))
                token_obj = result.scalar_one_or_none()
                if token_obj:
                    api_key = token_obj.token
                    # # print(f"API Key found: {api_key}")
        
        # Parse column mappings
        # # print("=== COLUMN MAPPING DEBUG ===")
        # # print(f"Variable ID (raw): '{variable_id}'")
        # # print(f"Variable Label (raw): '{variable_label}'")
        
        id_columns = [col.strip() for col in variable_id.split(',') if col.strip()]
        label_columns = [col.strip() for col in variable_label.split(',') if col.strip()]
        
        # print(f"ID columns: {id_columns}")
        # print(f"Label columns: {label_columns}")
        # print(f"ID columns length: {len(id_columns)}")
        # print(f"Label columns length: {len(label_columns)}")
        
        if len(id_columns) != len(label_columns):
            # print("Column lengths don't match, returning empty")
            return []
        
        column_mapping = dict(zip(id_columns, label_columns))
        # print(f"Column mapping: {column_mapping}")
        # print("=== END COLUMN MAPPING DEBUG ===")
        
        # Prepare headers for the request
        headers = {
            'Accept': 'application/json'
        }
        if api_key:
            headers['X-API-KEY'] = api_key
        # print(f"Headers: {headers}")
        # print("About to make HTTP request...")
        
        # Fetch data
        # # print("Starting HTTP request...")
        async with httpx.AsyncClient(verify=False) as client:
            # print("HTTP client created, making request...")
            response = await client.get(source_url, headers=headers)
            # print("HTTP request completed!")
            response.raise_for_status()
            data = response.json()
            
            # print(f"Response status: {response.status_code}")
            # print(f"Data type: {type(data)}")
            # print(f"Data content: {data}")
            # print(f"Data length: {len(data) if isinstance(data, list) else 'N/A'}")
            
            # The data is a list of dicts with keys: slevel, stime, sensor
            if not isinstance(data, list):
                # print("Data is not a list, returning empty")
                return []
        
        transformed_data = []
        for point in data:
            try:
                # convert stime to ISO 8601 UTC
                stime_raw = point.get('stime', '').strip()
                # Convert 'YYYY-MM-DD HH:MM:SS' to ISO 8601 with Z
                if stime_raw:
                    dt = datetime.strptime(stime_raw, "%Y-%m-%d %H:%M:%S")
                    iso_time = dt.isoformat(timespec='seconds') + "Z"
                else:
                    iso_time = ''
                # Build the mapping input dict
                station_mean = station.mean if hasattr(station, 'mean') else 0
                
                # calculate mean
                if station_mean != 0:
                    height = calculate_sea_level_mean(point.get('slevel'), station_mean)                    
                else:
                    height = point.get('slevel')

                mapping_input = {
                    'slevel': height,
                    'stime': iso_time,
                    'sensor': point.get('sensor'),
                    'lon_deg': station.longitude,
                    'lat_deg': station.latitude
                }
                # Map to output keys
                transformed_entry = {}
                for old_key, new_key in column_mapping.items():
                    if old_key in mapping_input:
                        transformed_entry[new_key] = mapping_input[old_key]
                transformed_data.append(transformed_entry)
            except Exception:
                continue
        
        # Sort by time (newest first)
        def parse_timestamp(entry):
            try:
                for key in entry:
                    if 'time' in key:
                        timestamp_str = entry[key]
                        if timestamp_str:
                            if 'Z' in timestamp_str:
                                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            elif '+' in timestamp_str:
                                return datetime.fromisoformat(timestamp_str)
                            else:
                                return datetime.fromisoformat(timestamp_str)
                return datetime.min
            except:
                return datetime.min
        
        sorted_data = sorted(
            transformed_data,
            key=lambda x: parse_timestamp(x),
            reverse=True
        )
        
        interval = int(getattr(station, 'intervals', 0) or 0)
        filtered_data = filter_bad_data(sorted_data[:limit], getattr(station, 'bad_data', None))
        
        # --- Outlier removal (IQR-based) ---
        OUTLIER_FACTOR = 1.5
        OUTLIER_MIN_SAMPLES = 12

        def _quantile_sorted(sorted_arr: list[float], q: float) -> float:
            if not sorted_arr:
                return float('nan')
            pos = (len(sorted_arr) - 1) * q
            base = int(math.floor(pos))
            rest = pos - base
            if base + 1 < len(sorted_arr):
                return sorted_arr[base] + rest * (sorted_arr[base + 1] - sorted_arr[base])
            else:
                return sorted_arr[base]

        def _iqr_thresholds(values: list[float], k: float = OUTLIER_FACTOR) -> tuple[float, float]:
            arr = [v for v in values if v is not None and isinstance(v, (int, float)) and math.isfinite(v)]
            if not arr:
                return (-float('inf'), float('inf'))
            arr.sort()
            q1 = _quantile_sorted(arr, 0.25)
            q3 = _quantile_sorted(arr, 0.75)
            iqr = q3 - q1
            lower = q1 - k * iqr
            upper = q3 + k * iqr
            return (lower, upper)

        def _is_sea_level_key(key: str) -> bool:
            if not key:
                return False
            kl = key.lower()
            # Treat common sea-level names; many stations label sea level as 'm'
            if kl in {'sea_level', 'slevel', 'm', 'sea level', 'water_level'}:
                return True
            return ('sea' in kl and 'level' in kl) or ('water' in kl and 'level' in kl)

        def _remove_outliers(values: list, key: str, station_id: str):
            # Convert incoming mixed values to floats where possible, else None
            num_vals = []
            for v in values:
                try:
                    # Treat sentinel -999 (and string forms) as missing
                    if v is None or v == -999 or v == "-999" or v == -999.0:
                        num_vals.append(None)
                    elif isinstance(v, (int, float)):
                        num_vals.append(float(v) if math.isfinite(v) else None)
                    else:
                        # Try to convert string to float
                        num_vals.append(float(v))
                except (ValueError, TypeError):
                    num_vals.append(None)

            non_null = [v for v in num_vals if v is not None and math.isfinite(v)]
            non_null_count = len(non_null)
            
            # Allow smaller minimum for sea level series to catch obvious spikes in shorter windows
            min_samples = 5 if _is_sea_level_key(key) else OUTLIER_MIN_SAMPLES
            if non_null_count < min_samples:
                # Too short to judge; return original values unchanged
                return values, 0, None, None

            lower, upper = _iqr_thresholds(num_vals)
            effective_lower = lower
            effective_upper = upper

            removed = 0
            cleaned = []
            
            # Domain-aware adjustment for sea level
            if _is_sea_level_key(key):
                sorted_vals = sorted(non_null)
                median = _quantile_sorted(sorted_vals, 0.5)
                
                # Calculate MAD (Median Absolute Deviation)
                abs_devs = [abs(v - median) for v in sorted_vals]
                mad = _quantile_sorted(sorted(abs_devs), 0.5) if abs_devs else 0.0
                
                # Heuristics for sea level
                domain_lower = 0.15  # meters
                median_fraction_lower = median * 0.25
                mad_boost_lower = (median - 6 * mad) if mad > 0 else (median * 0.4)
                q1 = _quantile_sorted(sorted_vals, 0.25)
                
                candidate_lower = max(lower, domain_lower, median_fraction_lower, mad_boost_lower)
                effective_lower = min(candidate_lower, q1 * 0.9)
                if effective_lower < lower:
                    effective_lower = lower

            # Apply outlier filtering
            for v in num_vals:
                if v is None or not isinstance(v, (int, float)) or not math.isfinite(v):
                    cleaned.append(None)
                    continue
                    
                # Sea level specific rules
                if _is_sea_level_key(key):
                    # Drop non-positive values for sea level
                    if v <= 0:
                        cleaned.append(None)
                        removed += 1
                        continue
                    # Apply domain-adjusted bounds
                    if v < effective_lower or v > effective_upper:
                        cleaned.append(None)
                        removed += 1
                    else:
                        cleaned.append(v)
                else:
                    # Standard IQR filtering for other variables
                    if v < lower or v > upper:
                        cleaned.append(None)
                        removed += 1
                    else:
                        cleaned.append(v)

            # Log results
            if removed > 0:
                lb = effective_lower if _is_sea_level_key(key) else lower
                logger.info(f"[outliers] station={station_id} variable={key} removed={removed} total={len(values)} thresholds=({lb:.3f}, {upper:.3f})")

            return cleaned, removed, effective_lower, upper

        # Build per-key arrays and apply outlier removal
        if filtered_data:
            time_fields = {'time', 'timestamp', 'obs_time_utc', 'stime', 'date', 'datetime'}
            exclude_fields = time_fields | {'lon_deg', 'lat_deg', 'longitude', 'latitude', 'sensor'}
            
            # Collect keys to process (numeric-ish)
            candidate_keys = set()
            for row in filtered_data:
                for k, v in row.items():
                    if k in exclude_fields:
                        continue
                    # Consider numeric-looking keys only
                    if isinstance(v, (int, float)):
                        candidate_keys.add(k)
                    else:
                        try:
                            float(v)
                            candidate_keys.add(k)
                        except (ValueError, TypeError):
                            pass

            # For each key, build series and clean
            removed_counts = {}
            for key in candidate_keys:
                series = []
                for row in filtered_data:
                    series.append(row.get(key))
                
                cleaned_series, removed, _, _ = _remove_outliers(
                    series, key, getattr(station, 'station_id', 'unknown')
                )
                removed_counts[key] = removed
                
                # Write back cleaned values
                for idx, val in enumerate(cleaned_series):
                    if idx < len(filtered_data) and key in filtered_data[idx]:
                        filtered_data[idx][key] = val

            # Summary log across variables
            total_removed = sum(removed_counts.values())
            if total_removed > 0:
                parts = ", ".join([f"{k}={v}" for k, v in removed_counts.items() if v > 0])
                logger.info(f"[outliers] station={getattr(station, 'station_id', 'unknown')} total_removed={total_removed} by_variable: {parts}")

            # If any sea-level key became None, drop the entire row for IOC output
            sea_keys = [k for k in candidate_keys if _is_sea_level_key(k)]
            if sea_keys:
                before = len(filtered_data)
                filtered_data = [
                    row for row in filtered_data 
                    if any(row.get(k) is not None for k in sea_keys)
                ]
                dropped = before - len(filtered_data)
                if dropped > 0:
                    logger.info(f"[outliers] station={getattr(station, 'station_id', 'unknown')} dropped_rows={dropped} due_to_sea_level_null")
        # --- End outlier removal ---

        # Datetime filter
        if start or end:
            def in_range(entry):
                t = entry.get('time') or entry.get('timestamp') or entry.get('obs_time_utc') or entry.get('stime') or entry.get('date') or entry.get('datetime')
                if not t:
                    return False
                try:
                    dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                except:
                    return False
                if start:
                    try:
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        if dt < start_dt:
                            return False
                    except:
                        pass
                if end:
                    try:
                        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        if dt > end_dt:
                            return False
                    except:
                        pass
                return True
            filtered_data = [d for d in filtered_data if in_range(d)]
        
        return apply_intervals(filtered_data, interval)
        
    except Exception as e:
        logger.error(f"=== IOC METHOD ERROR: {str(e)} ===")
        import traceback
        logger.error(traceback.format_exc())
        return []


async def refresh_neon_token() -> dict:
    """
    Authenticate with NEON API and return a new token.
    Automatically updates the token in the database.
    
    Returns:
        dict with keys: { 'token': str, 'session_id': str }
    """
    login_url = "https://restservice-neon.niwa.co.nz/NeonRESTService.svc/PostSession"
    
    async with AsyncSessionLocal() as db:
        # Get NEON credentials
        result = await db.execute(
            select(Token).where(Token.comments == "neon_cred")
        )
        neon_cred_obj = result.scalar_one_or_none()
        
        if not neon_cred_obj:
            print("Error: neon_cred not found in database")
            return None
        
        # Parse credentials (format: username/password)
        cred_parts = neon_cred_obj.token.split('/')
        if len(cred_parts) != 2:
            print("Error: neon_cred format invalid. Expected 'username/password'")
            return None
        
        username, password = cred_parts[0].strip(), cred_parts[1].strip()
        
        payload = {
            "Username": username,
            "Password": password
        }
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.post(login_url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                new_token = data.get("Token")
                session_id = response.cookies.get("ASP.NET_SessionId")
                
                if not new_token:
                    print("Error: No token received from NEON API")
                    return None
                if not session_id:
                    print("Error: No ASP.NET_SessionId cookie received from NEON API")
                    return None
                
                # Update the neon_token in database
                result = await db.execute(
                    select(Token).where(Token.comments == "neon_token")
                )
                token_obj = result.scalar_one_or_none()
                
                if token_obj:
                    token_obj.token = new_token
                    await db.commit()
                else:
                    print("Error: neon_token entry not found in database")
                    return None

                # Upsert the neon_cookie (ASP.NET_SessionId)
                result_cookie = await db.execute(
                    select(Token).where(Token.comments == "neon_cookie")
                )
                cookie_obj = result_cookie.scalar_one_or_none()
                if cookie_obj:
                    cookie_obj.token = session_id
                else:
                    cookie_obj = Token(token=session_id, comments="neon_cookie")
                    db.add(cookie_obj)
                await db.commit()
                
                return {"token": new_token, "session_id": session_id}
                
        except httpx.HTTPStatusError as e:
            print(f"NEON authentication failed: {e.response.status_code} - {str(e)}")
            return None
        except Exception as e:
            print(f"Error refreshing NEON token: {str(e)}")
            return None


async def actual_neon_method(station, token: str, session_id: str, limit=100, start: str = None, end: str = None) -> List[dict]:
    """
    Fetch and process data from NIWA NEON REST service.
    Supports multiple URLs separated by | for merging datasets (e.g., sea level + temperature).
    
    Args:
        station: Station object with configuration
        token: Valid NEON authentication token
        session_id: ASP.NET session cookie
        limit: Maximum number of records to return
        start: Start datetime filter (ISO format)
        end: End datetime filter (ISO format)
    
    Returns:
        List of processed data records merged by timestamp
    """
    # Build URL and mapping; let HTTP errors bubble up so caller can refresh token
    source_url = station.source_url
    variable_id = station.variable_id or ''
    variable_label = station.variable_label or ''

    # Calculate date range per requirement
    now = datetime.now()
    # End time: today at the max time (23:59:59) - end of day
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=0)
    # Start time: 5 days ago at the beginning (00:00:00) - start of day
    start_time = (now - timedelta(days=5)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Format times for URL (no colon encoding to match expected format)
    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    # Split multiple URLs (if any) separated by |
    urls = [u.strip() for u in source_url.split('|') if u.strip()]
    
    # Parse column mappings - expect format like "Time,Value,Time,Value" for multiple URLs
    # and labels like "time,sea_value,time,sea_temp"
    id_columns = [col.strip() for col in variable_id.split(',') if col.strip()]
    label_columns = [col.strip() for col in variable_label.split(',') if col.strip()]

    if len(id_columns) != len(label_columns):
        print("Error: Column mapping mismatch")
        return []

    # Prepare headers and cookies per NEON requirements
    headers = {
        'X-Authentication-Token': str(token),
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    cookies = {}
    if session_id:
        cookies["ASP.NET_SessionId"] = str(session_id)

    # Fetch data from each URL and extract dataset IDs
    all_datasets = []
    
    try:
        async with httpx.AsyncClient(verify=False) as client:
            for idx, url_template in enumerate(urls):
                # Replace placeholders in URL
                url = url_template.replace("START_TIME", start_time_str)
                url = url.replace("END_TIME", end_time_str)
                
                # Ensure query params are correctly separated
                url = re.sub(r'(?<![&?])Interval=', r'&Interval=', url)
                url = re.sub(r'(?<![&?])Method=', r'&Method=', url)
                
                # Extract dataset ID from URL (e.g., GetDataResampled/273417 -> 273417)
                dataset_id_match = re.search(r'/GetDataResampled/(\d+)', url)
                dataset_id = dataset_id_match.group(1) if dataset_id_match else None
                
                # Print the final URL being requested for debugging/visibility
                # try:
                #     print(f"NEON request URL [{idx+1}/{len(urls)}]: {url}")
                # except Exception:
                #     pass
                
                # Make API call (let HTTPStatusError propagate)
                response = await client.get(url, headers=headers, cookies=cookies)
                response.raise_for_status()
                data = response.json()
                
                # Extract samples
                result_data = data.get("GetDataResampledResult", {}) if isinstance(data, dict) else {}
                samples = result_data.get("Samples", []) if isinstance(result_data, dict) else []
                
                
                # Store dataset with its ID for mapping later
                all_datasets.append({
                    'dataset_id': dataset_id,
                    'samples': samples
                })
        
        # Now merge datasets by timestamp
        # Map datasets to labels in order: first non-time label goes to first URL, etc.
        
        
        
        # Find time label and value labels
        time_label = None
        value_labels = []
        
        for label in label_columns:
            if 'time' in label.lower():
                time_label = label
            else:
                value_labels.append(label)
        
        if not time_label:
            time_label = "time"  # fallback
        
        # Try to match by dataset ID in label first
        dataset_label_map = {}
        for label in value_labels:
            id_match = re.search(r'_(\d+)$', label)
            if id_match:
                dataset_id = id_match.group(1)
                dataset_label_map[dataset_id] = label
        
        # If no ID-based mapping, use position-based mapping
        if not dataset_label_map:
            for idx, dataset_info in enumerate(all_datasets):
                if idx < len(value_labels):
                    dataset_label_map[dataset_info['dataset_id']] = value_labels[idx]
                    
        
        
        # Build time-indexed data structure
        merged_by_time = {}
        
        for dataset_info in all_datasets:
            dataset_id = dataset_info['dataset_id']
            samples = dataset_info['samples']
            
            # Find the corresponding value label for this dataset
            value_label = dataset_label_map.get(dataset_id)
            
            if not value_label:
                continue
            
            sample_count = 0
            for sample in samples:
                raw_val = sample.get('Value')
                try:
                    val_num = float(raw_val) if raw_val is not None and raw_val != '' else None
                except:
                    val_num = None
                
                time_val = sample.get('Time')
                time_norm = to_iso_z(time_val)
                
                if not time_norm:
                    continue
                
                # Initialize or update entry for this timestamp
                if time_norm not in merged_by_time:
                    merged_by_time[time_norm] = {
                        time_label: time_norm
                    }
                
                # Add the value field with its specific label
                final_val = val_num if val_num is not None else raw_val
                merged_by_time[time_norm][value_label] = final_val
                
                sample_count += 1
                # Debug: log first few entries
                
        
        # Convert merged data to list
        transformed_data = list(merged_by_time.values())

        # sea_level, sea_temp, time
        if transformed_data:
            desired_order = [lbl for lbl in value_labels] + [time_label]
            ordered_rows = []
            for row in transformed_data:
                new_row = {}
             
                for k in desired_order:
                    if k in row:
                        new_row[k] = row[k]
               
                for k, v in row.items():
                    if k not in new_row:
                        new_row[k] = v
                ordered_rows.append(new_row)
            transformed_data = ordered_rows

        # Simple unit normalization: NEON returns sea_level in millimeters; convert to meters.
        # Per user request, no heuristics or config—just divide any 'sea_level' field by 1000.
        for row in transformed_data:
            for key in list(row.keys()):
                if key.lower() == 'sea_level':
                    try:
                        row[key] = float(row[key]) / 1000.0
                    except Exception:
                        # Leave value unchanged if not numeric
                        pass

  
        try:
            sea_level_key_candidates = [k for k in (value_labels or []) if 'sea_level' in k.lower() or k.lower() == 'slevel']
            if transformed_data and sea_level_key_candidates:
                divisor = None
                # Prefer explicit attribute if present
                if hasattr(station, 'sea_level_divisor') and getattr(station, 'sea_level_divisor'):
                    try:
                        divisor = float(getattr(station, 'sea_level_divisor'))
                    except Exception:
                        divisor = None
                # Heuristic detection if no explicit divisor
                if divisor is None:
                    # Look at first 20 values to guess scale
                    sample_vals = []
                    for row in transformed_data[:20]:
                        for key in sea_level_key_candidates:
                            if key in row:
                                try:
                                    sample_vals.append(float(row[key]))
                                except Exception:
                                    pass
                    if sample_vals and any(v > 50 for v in sample_vals):  # >50 m improbable => likely mm
                        divisor = 1000.0
                if divisor and divisor != 0:
                    for row in transformed_data:
                        for key in sea_level_key_candidates:
                            if key in row:
                                try:
                                    row[key] = float(row[key]) / divisor
                                except Exception:
                                    # leave value as-is if not convertible
                                    pass
        except Exception:
            # Never fail entire method due to scaling logic
            pass
        
        if not transformed_data:
            return []
        
        # Sort by timestamp (newest first)
        def parse_timestamp(entry):
            try:
                # Prefer the configured time label; fallback to common keys
                t = entry.get(time_label) or entry.get('time') or entry.get('timestamp')
                if t:
                    return datetime.fromisoformat(str(t).replace('Z', '+00:00'))
                return datetime.min
            except:
                return datetime.min
        
        sorted_data = sorted(
            transformed_data,
            key=lambda x: parse_timestamp(x),
            reverse=True
        )
        
        interval = int(getattr(station, 'intervals', 0) or 0)
        filtered_data = filter_bad_data(sorted_data[:limit], getattr(station, 'bad_data', None))
        
        # Optional datetime filter using query params
        if start or end:
            def in_range(entry):
                t = entry.get(time_label) or entry.get('time') or entry.get('timestamp')
                if not t:
                    return False
                try:
                    dt = datetime.fromisoformat(str(t).replace('Z', '+00:00'))
                except:
                    return False
                if start:
                    try:
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        if dt < start_dt:
                            return False
                    except:
                        pass
                if end:
                    try:
                        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        if dt > end_dt:
                            return False
                    except:
                        pass
                return True
            filtered_data = [d for d in filtered_data if in_range(d)]
        
        return apply_intervals(filtered_data, interval)
        
    except httpx.HTTPStatusError as e:        
        raise
    except Exception as e:
        print(f"Error in actual_neon_method (transform): {str(e)}")
        return []


async def neon_method(station, limit=100, start: str = None, end: str = None) -> List[dict]:
    """
    Main NEON method with automatic token refresh on authentication failure.
    
    Flow:
    1. Try with existing token
    2. If 401 (auth failed), refresh token and retry
    3. Process and return data
    """
    try:
        # Get current NEON token and session cookie from database
        async with AsyncSessionLocal() as db:
            result_tok = await db.execute(select(Token).where(Token.comments == "neon_token"))
            neon_token_obj = result_tok.scalar_one_or_none()
            result_cookie = await db.execute(select(Token).where(Token.comments == "neon_cookie"))
            neon_cookie_obj = result_cookie.scalar_one_or_none()

            if not neon_token_obj or not neon_token_obj.token:
                refreshed = await refresh_neon_token()
                if not refreshed:
                    return []
                current_token = refreshed.get("token")
                current_cookie = refreshed.get("session_id")
            else:
                current_token = neon_token_obj.token
                current_cookie = neon_cookie_obj.token if neon_cookie_obj else None

    # Attempt actual call with existing token and cookie
        return await actual_neon_method(station, current_token, current_cookie, limit, start, end)

    except httpx.HTTPStatusError as e:
        # If auth failed, refresh token and retry once
        if e.response is not None and e.response.status_code == 401:
            refreshed = await refresh_neon_token()
            if not refreshed:
                return []
            try:
                return await actual_neon_method(station, refreshed.get("token"), refreshed.get("session_id"), limit, start, end)
            except httpx.HTTPStatusError as e2:
                return []
        else:
            return []
    except Exception as e:
        print(f"Error in neon_method: {str(e)}")
        return []

METHOD_MAPPING = {
    "spot_method": spot_method,
    "pacioos_method": pacioos_method,
    "dart_method": dart_method,  # applied mean
    "ioc_method": ioc_method,    # applied mean
    "neon_method": neon_method,
}