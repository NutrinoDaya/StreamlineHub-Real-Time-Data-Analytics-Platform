#!/usr/bin/env python
"""
Traffic Metrics API

Endpoints for traffic statistics based on pre-aggregated
data windows stored in Elasticsearch.
"""

from collections import defaultdict
from fastapi import APIRouter, Query, HTTPException, Request
from datetime import datetime, timedelta
import math, calendar, sys
from pathlib import Path
from elasticsearch import Elasticsearch

router = APIRouter()

# ──────────────────────────────────────────────────────────────────────────────
# Setup & Imports
# ──────────────────────────────────────────────────────────────────────────────
root_dir = Path(__file__).parent.parent.parent
sys.path.append(str(root_dir))
from utils import readConfig, LoggerManager, DefaultResponseLoader
from utils.decrypt import decrypt_token_and_validate_roles, get_user_data_by_username, process_token


# ──────────────────────────────────────────────────────────────────────────────
# Elasticsearch Client
# ──────────────────────────────────────────────────────────────────────────────
conf = readConfig(root_dir / "config" / "Elasticsearch.xml")["Elasticsearch"]
es = Elasticsearch(
    hosts=[{"host": conf["es_host"], "port": int(conf["es_port"]), "scheme": "http"}],
    http_auth=(conf.get("es_user"), conf.get("es_password")) if conf.get("es_user") else None,
    timeout=360, max_retries=3, retry_on_timeout=True
)
logger = LoggerManager.get_logger("Query_Api.log")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def compute_range(val, step):
    lo = int(val // step) * step
    return f"{lo} - {lo + step}"

def compute_vehicle_m_range(count):
    m = count / 1e6
    lo = math.floor(m * 100) / 100.0
    return f"{lo:.2f}M - {lo+0.005:.3f}M"

def compute_vehicle_m_str(count):
    # for totalKPI
    return f"{count/1e6:.3f}M"

def zero_fill_24(buckets, key_func, val_func):
    """Return list of 24 dicts with keys 'hour' and a single value."""
    m = { key_func(b): val_func(b) for b in buckets }
    out = []
    for hr in range(24):
        out.append({"hour": hr, list(m.values())[0].__class__.__name__: m.get(hr, 0)})
    return out


@router.get("/Statistic")
def get_traffic_metrics(
    request: Request,
    StartDate:    str = Query(..., description="Start Date (YYYY-MM-DD) or ALL"),
    EndDate:      str = Query(..., description="End Date (YYYY-MM-DD) or ALL"),
    Region:       str = Query("All", description="Region filter"),
    VehicleClass: str = Query("All", description="Vehicle class filter"),
):
    filters = []

    # 1) Parse & validate dates
    if not (StartDate.upper() == "ALL" or EndDate.upper() == "ALL"):
        try:
            sd = datetime.strptime(StartDate, "%Y-%m-%d").date()
            ed = datetime.strptime(EndDate,   "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(400, "Dates must be YYYY-MM-DD or ALL")
        if ed < sd:
            raise HTTPException(400, "EndDate cannot be before StartDate")

        # build inclusive list of days in the same format your docs use e.g. "2025-5-27"
        days = []
        cur = sd
        while cur <= ed:
            days.append(f"{cur.year}-{cur.month}-{cur.day}")
            cur += timedelta(days=1)
        filters.append({
            "terms": {
                "processing_date.keyword": days
            }
        })
    
    # 2) other filters...
    if Region.upper() != "ALL":
        filters.append({"term": {"Region.keyword": Region}})
    if VehicleClass.upper() != "ALL":
        filters.append({"term": {"VehicleClass.keyword": VehicleClass}})

    # 3) Fetch up to 10000 matched docs
    body = {"query": {"bool": {"filter": filters}}}
    try:
        resp = es.search(
            index="traffic_data_aggregated",
            body=body,
            size=10000,
            _source=[
                "window_start","processing_date","avg_speed",
                "vehicle_count","violation_count",
                "VehicleClass","Location0","SpeedLimit"
            ]
        )
    except Exception as e:
        logger.error(f"ES query failed: {e}")
        raise HTTPException(500, "Elasticsearch query failed")

    hits = resp.get("hits", {}).get("hits", [])

    if not hits:
        traffic_data = DefaultResponseLoader.load_default_response("Traffic")
        print("Loaded Traffic default response data:", traffic_data)
            
        # Extract and validate Authorization token
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid or missing Authorization header")

        token = auth_header.replace("Bearer ", "")
        try:
            user_data = decrypt_token_and_validate_roles(token)
            if not user_data:
                raise HTTPException(status_code=401, detail="Invalid token or insufficient permissions")
        except Exception as e:
            logger.error(f"Token decryption failed: {e}")
            raise HTTPException(status_code=401, detail="Token decryption failed")

        # Get username from user_data
        username = user_data.get("Name", "")
        if not username:
            raise HTTPException(status_code=401, detail="No username found in token")

        # Get user data from CSV using username
        csv_user_data = get_user_data_by_username(username)
        if not csv_user_data:
            raise HTTPException(status_code=401, detail="No user data found in CSV; please log in.")

        # Get token from CSV and validate it
        csv_token = csv_user_data.get("token")
        if not csv_token:
            raise HTTPException(status_code=401, detail="No token found in CSV for user")

        # Validate token using process_token
        validated_token = process_token(csv_token)

        return {
            "status": True,
            "message": "No data found",
            "data": traffic_data,
            "authenticationToken": validated_token
        }
    # 3) Materialize source records
    recs = [h["_source"] for h in hits]

    # 4) Build helper maps
    by_hour = defaultdict(list)
    by_month = defaultdict(list)
    by_class = defaultdict(list)
    veh_by_class  = defaultdict(int)
    veh_by_hour = defaultdict(int)
    veh_by_month = defaultdict(int)
    viol_by_class = defaultdict(int)
    viol_by_hour_class = defaultdict(lambda: defaultdict(int))
    # for limit/location
    speeds_by_loc = defaultdict(list)

    for r in recs:
         # hour from window_start
         hr = datetime.fromtimestamp(r["window_start"]/1000).hour
         by_hour[hr].append(r["avg_speed"])
         veh_by_hour[hr] += r["vehicle_count"]
         # group by month/year from processing_date
         pd = datetime.strptime(r["processing_date"], "%Y-%m-%d")
         mkey = (pd.year, pd.month)
         by_month[mkey].append(r["avg_speed"])
         veh_by_month[mkey] += r["vehicle_count"]
         # by class (for avg speeds)
         cls = r["VehicleClass"]
         by_class[cls].append(r["avg_speed"])
         # total vehicles by class
         veh_by_class[cls] += r["vehicle_count"]
         # violations by class
         viol_by_class[cls] += r["violation_count"]
         # violations by hour/class
         viol_by_hour_class[hr][cls] += r["violation_count"]
         # speed by location
         speeds_by_loc[r["Location0"]].append(r["avg_speed"])

    # 5) averageSpeedsByTime
    avg_by_hour = [
        {"hour": hr, "avgHistSpeed": f"{(sum(speeds)/len(speeds)):.1f}"}
        if hr in by_hour else {"hour": hr, "avgHistSpeed": "0.0"}
        for hr in range(24)
        for speeds in ([by_hour[hr]] if hr in by_hour else [[]])
    ]

    # 6) averageSpeedsByMonth
    avg_by_month = []
    for (y,m), speeds in sorted(by_month.items()):
        avg_by_month.append({
            "year": str(y),
            "month": calendar.month_name[m],
            "avgHistSpeed": f"{sum(speeds)/len(speeds):.1f}"
        })

    # 7) averageSpeedsByVehicleClass
    avg_by_class = []
    for cls, speeds in by_class.items():
        avg = sum(speeds)/len(speeds)
        avg_by_class.append({
            "vehicleClass": cls,
            "avgHistSpeed": f"{avg:.1f}",
            "avgHistSpeedRange": compute_range(avg, 5)
        })

    # 8) vehicleByTime
    veh_time = [{"hour": hr, "vehicleCount": str(veh_by_hour.get(hr, 0))}
                for hr in range(24)]

    # 9) vehicleByMonth
    veh_month = [{
        "year": str(y), "month": calendar.month_name[m],
        "vehicleCount": str(c)
    } for (y,m), c in sorted(veh_by_month.items())]

    # 10) trafficViolationsByVehicleClass
    viol_cls = [{"vehicleClass": cls.lower(), "caseCount": str(c)}
                for cls,c in viol_by_class.items()]

    # 11) trafficViolationsByTime
    tv_by_time = []
    for hr, cls_map in viol_by_hour_class.items():
        for cls, c in cls_map.items():
            tv_by_time.append({
                "hour": f"{hr:02d}",
                "vehicleClass": cls.lower(),
                "trafficCount": str(c)
            })

    
    # 12) trafficViolationsByMonth
    # build a map: { vehicleClass → { monthName → totalViolations } }
    viol_by_month_class = defaultdict(lambda: defaultdict(int))
    for r in recs:
        cls = r["VehicleClass"].lower()
        # derive month from your timestamp; here using window_start
        dt = datetime.fromtimestamp(r["window_start"] / 1000)
        month_name = calendar.month_name[dt.month]
        viol_by_month_class[cls][month_name] += r.get("violation_count", 0)

    trafficViolationsByMonth = []
    for cls, month_map in viol_by_month_class.items():
        for month, cnt in month_map.items():
            trafficViolationsByMonth.append({
                "trafficCount": str(cnt),
                "month": month,
                "vehicleClass": cls
            })


    # 12) averageSpeedBySpeedLimit
    all_speeds = [r["avg_speed"] for r in recs]
    avg_all = sum(all_speeds)/len(all_speeds)
    speed_limit_metric = [{
        "speedLimit": None,
        "avgHistSpeed": f"{avg_all:.1f}",
        "avgHistSpeedRange": compute_range(avg_all, 5)
    }]

    # 13) averageSpeedBySpeedLimitAndLocation
    avg_by_loc = []
    for loc, speeds in speeds_by_loc.items():
        avg = sum(speeds)/len(speeds)
        avg_by_loc.append({
            "speedLimit": None,
            "locationKey": loc,
            "avgHistSpeed": f"{avg:.1f}",
            "avgHistSpeedRange": compute_range(avg, 5)
        })

    # 14) peakHour (hour with max vehicle_count)
    peak_hr, peak_cnt = max(veh_by_hour.items(), key=lambda x: x[1])
    peakHour = [{
        "hour": str(peak_hr),
        "avgHistVehicleCount": f"{peak_cnt:.1f}"
    }]

    # 15) topViolationLocation
    loc_viol = defaultdict(int)
    for r in recs:
        loc_viol[r["Location0"]] += r["violation_count"]
    top_violation = sorted(loc_viol.items(), key=lambda x: x[1], reverse=True)[:1]
    topViolationLocation = [{
        "locationKey": loc, "caseCount": str(c)
    } for loc,c in top_violation]

    # 16) highestVehicleCountLocation
    loc_veh = defaultdict(int)
    for r in recs:
        loc_veh[r["Location0"]] += r["vehicle_count"]
    top_veh_loc, top_veh_cnt = max(loc_veh.items(), key=lambda x: x[1])
    highestVehicleCountLocation = [{
        "locationKey": top_veh_loc,
        "vehicleCount": str(top_veh_cnt),
        "vehicleCountRange": compute_vehicle_m_range(top_veh_cnt)
    }]

    # 17) top5LocationByHighestAverageSpeed / Lowest
    avg_loc_list = [
        (loc, sum(speeds)/len(speeds)) for loc,speeds in speeds_by_loc.items()
    ]
    top5_high = sorted(avg_loc_list, key=lambda x: x[1], reverse=True)[:5]
    top5_low  = sorted(avg_loc_list, key=lambda x: x[1])[:5]
    top5_high_speed_loc = [{
        "locationKey": loc,
        "avgHistSpeed": f"{avg:.1f}",
        "avgHistSpeedRange": compute_range(avg,5)
    } for loc,avg in top5_high]
    top5_low_speed_loc = [{
        "locationKey": loc,
        "avgHistSpeed": f"{avg:.1f}",
        "avgHistSpeedRange": compute_range(avg,5)
    } for loc,avg in top5_low]

    # 18) top5LocationByViolation
    top5_viols = sorted(loc_viol.items(), key=lambda x: x[1], reverse=True)[:5]
    top5LocationByViolation = [{
        "locationKey": loc, "caseCount": str(c)
    } for loc,c in top5_viols]

    # 19) top5DateByTrafficValume
    top5_dates = sorted(veh_by_month.items(), key=lambda x: x[1], reverse=True)[:5]

    top5DateByTrafficValume = []
    # 20) top5LocationByTrafficValume
    top5_loc_vol = sorted(loc_veh.items(), key=lambda x: x[1], reverse=True)[:5]
    top5LocationByTrafficValume = [{
        "locationKey": loc,
        "vehicleCount": str(cnt),
        "vehicleCountRange": compute_vehicle_m_range(cnt)
    } for loc,cnt in top5_loc_vol]

    # # 21) top5LocationByTime: one best location per hour, then pick top5
    # per_hour_best = []
    # for hr, group in defaultdict(lambda: defaultdict(int),
    #                             { hr: {} for hr in range(24) }).items():
    #     # build map hr -> loc -> sum count
    #     pass

    # better: reuse veh_by_hour and loc_veh per hour
    # we already have veh_by_hour per hour total; need per (hr,loc)
    hr_loc = defaultdict(lambda: defaultdict(int))
    for r in recs:
        hr = datetime.fromtimestamp(r["window_start"]/1000).hour
        hr_loc[hr][r["Location0"]] += r["vehicle_count"]

    per_hour_top = []
    for hr, loc_map in hr_loc.items():
        best_loc, best_cnt = max(loc_map.items(), key=lambda x: x[1])
        per_hour_top.append((hr,best_loc,best_cnt))
    # pick top 5 by count
    top5LocationByTime = [{
        "hour": hr,
        "locationKey": loc,
        "caseCount": str(cnt)
    } for hr,loc,cnt in sorted(per_hour_top, key=lambda x: x[2], reverse=True)[:5]]

    # 22) totalKPI
    totalKPI = [{
        "avgHistSpeed": f"{avg_all:.1f}",
        "vehicleCount": compute_vehicle_m_str(sum(r["vehicle_count"] for r in recs))
    }]
    vehicleByVehicleClass = [
        {
            "vehicleClass": cls.lower(),
            "vehicleCount": str(count),
            "avgHistVehicleCountRange": compute_vehicle_m_range(count)
        }
        for cls, count in veh_by_class.items()
    ]
    data = {
        "averageSpeedsByTime":             avg_by_hour,
        "averageSpeedsByMonth":            avg_by_month,
        "averageSpeedsByVehicleClass":     avg_by_class,
        "vehicleByVehicleClass":           vehicleByVehicleClass,
        "vehicleByTime":                   veh_time,
        "vehicleByMonth":                  veh_month,
        "trafficViolationsByVehicleClass": viol_cls,
        "trafficViolationsByTime":         tv_by_time,
        "trafficViolationsByMonth":        trafficViolationsByMonth, 
        "averageSpeedBySpeedLimit":        speed_limit_metric,
        "averageSpeedBySpeedLimitAndLocation": avg_by_loc,
        "peakHour":                        peakHour,
        "topViolationLocation":            topViolationLocation,
        "highestVehicleCountLocation":     highestVehicleCountLocation,
        "top5LocationByHighestAverageSpeed": top5_high_speed_loc,
        "top5LocationByLowestAverageSpeed":  top5_low_speed_loc,
        "top5LocationByViolation":           top5LocationByViolation,
        "top5DateByTrafficValume":           top5DateByTrafficValume,
        "top5LocationByTrafficValume":       top5LocationByTrafficValume,
        "top5LocationByTime":                top5LocationByTime,
        "totalKPI":                          totalKPI,
        "siteList":                          []
    }

    # Extract and validate Authorization token
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing Authorization header")

    token = auth_header.replace("Bearer ", "")
    try:
        user_data = decrypt_token_and_validate_roles(token)
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid token or insufficient permissions")
    except Exception as e:
        logger.error(f"Token decryption failed: {e}")
        raise HTTPException(status_code=401, detail="Token decryption failed")

    # Get username from user_data
    username = user_data.get("Name", "")
    if not username:
        raise HTTPException(status_code=401, detail="No username found in token")

    # Get user data from CSV using username
    csv_user_data = get_user_data_by_username(username)
    if not csv_user_data:
        raise HTTPException(status_code=401, detail="No user data found in CSV; please log in.")

    # Get token from CSV and validate it
    csv_token = csv_user_data.get("token")
    if not csv_token:
        raise HTTPException(status_code=401, detail="No token found in CSV for user")

    # Validate token using process_token
    validated_token = process_token(csv_token)

    return {
        "status": True,
        "message": "Item Found - 'VSB.AnalyticsApp.WebApi.Types.TrafficTypes.TrafficStatsDto'",
        "data": data,
        "authenticationToken":validated_token 
    }


@router.get("/DevicesPerformanceWithPagination")
def get_devices_performance_with_pagination(
    request: Request,
    DeviceIndex: str = Query("All", description="Filter by Device Index"),
    StartDate: str = Query("All", description="Start Date (YYYY-MM-DD) or ALL"),
    EndDate: str = Query("All", description="End Date (YYYY-MM-DD) or ALL"),
    PageNumber: int = Query(1, ge=1, description="Page number"),
    PageSize: int = Query(4, ge=0, description="Number of items per page. Use 0 to return all data")
):
    logger.info(
        f"Received DevicesPerformanceWithPagination request: "
        f"DeviceIndex={DeviceIndex}, StartDate={StartDate}, EndDate={EndDate}, "
        f"PageNumber={PageNumber}, PageSize={PageSize}"
    )

    # 1) Parse date filters
    if StartDate.upper() == "ALL" or EndDate.upper() == "ALL":
        range_start = None
        range_end = None
    else:
        try:
            sd = datetime.strptime(StartDate, "%Y-%m-%d")
            ed = datetime.strptime(EndDate, "%Y-%m-%d") + timedelta(days=1) - timedelta(milliseconds=1)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        if ed < sd:
            raise HTTPException(status_code=400, detail="End date cannot be before start date")
        range_start = int(sd.timestamp() * 1000)
        range_end = int(ed.timestamp() * 1000)

    # 2) Build the filtered ES query for data
    must_clauses = []
    if DeviceIndex.upper() != "ALL":
        must_clauses.append({"term": {"DeviceIndex.keyword": DeviceIndex}})
    if range_start is not None and range_end is not None:
        must_clauses.append({"range": {"window_start": {"gte": range_start, "lte": range_end}}})
    data_query = {"query": {"bool": {"must": must_clauses}}} if must_clauses else {"query": {"match_all": {}}}

    try:
        resp = es.search(index="traffic_data_aggregated", body=data_query, size=10000)
    except Exception as e:
        logger.error(f"Error querying traffic_data_aggregated for data: {e}")
        raise HTTPException(status_code=500, detail="Elasticsearch data query failed")

    hits = resp.get("hits", {}).get("hits", [])
    records = [h["_source"] for h in hits]

    # 3) Fetch device list for filters
    try:
        all_resp = es.search(
            index="traffic_data_aggregated",
            body={"query": {"match_all": {}}},
            size=10000,
            _source=["DeviceIndex", "Location0"]
        )
    except Exception as e:
        logger.error(f"Error querying traffic_data_aggregated for filter list: {e}")
        all_resp = {"hits": {"hits": []}}

    seen = set()
    deviceFilterList = []
    for h in all_resp["hits"]["hits"]:
        src = h.get("_source", {})
        dev = src.get("DeviceIndex")
        loc = src.get("Location0", "")
        if not dev:
            continue
        key = (dev, loc)
        if key in seen:
            continue
        seen.add(key)
        deviceFilterList.append({
            "deviceIndex": dev,
            "locationIndex": loc,
            "sessionStartDate": "0001-01-01T00:00:00"
        })

    # 4) Group data by DeviceIndex and Location
    grouped = defaultdict(list)
    for r in records:
        dev = r.get("DeviceIndex")
        loc = r.get("Location0", "")
        ws = r.get("window_start")
        we = r.get("window_end")
        if not dev or ws is None or we is None:
            continue
        grouped[(dev, loc)].append((ws, we))

    deviceData = []
    for (dev, loc), windows in grouped.items():
        windows.sort()
        min_ts = min(ws for ws, _ in windows)
        max_ts = max(we for _, we in windows)

        start_dt = datetime.fromtimestamp(min_ts / 1000)
        end_dt = datetime.fromtimestamp(max_ts / 1000)

        day_totals = defaultdict(float)
        current = start_dt
        while current.date() <= end_dt.date():
            next_day = datetime.combine(current.date() + timedelta(days=1), datetime.min.time())
            day_start = max(current, start_dt)
            day_end = min(end_dt, next_day)
            duration_seconds = (day_end - day_start).total_seconds()
            duration_hours = duration_seconds / 3600
            day_totals[current.date()] += duration_hours
            current = next_day

        monthly = defaultdict(lambda: defaultdict(float))
        for day, hrs in day_totals.items():
            monthly[(day.year, day.month)][day.day] += hrs

        for (y, m), days_dict in monthly.items():
            sessionData = {}
            num_days = calendar.monthrange(y, m)[1]
            graph = [0.0] * num_days
            for day, total_hrs in days_dict.items():
                rounded_hrs = round(total_hrs, 2)
                sessionData[f"Day {day}"] = {
                    "hours": rounded_hrs,
                    "busSpeed": "",
                    "carSpeed": "",
                    "truckSpeed": "",
                    "busTrigger": "",
                    "carTrigger": "",
                    "truckTrigger": ""
                }
                graph[day - 1] = rounded_hrs

            deviceData.append({
                "month": m,
                "year": y,
                "deviceIndex": dev,
                "locationIndex": loc,
                "sessionData": sessionData,
                "graph": graph
            })

    total_count = len(deviceData)
    if PageSize == 0:
        paginated_data = deviceData
    else:
        start_idx = (PageNumber - 1) * PageSize
        end_idx = start_idx + PageSize
        paginated_data = deviceData[start_idx:end_idx]

    return {
        "status": True,
        "message": "Item Found - 'VSB.AnalyticsApp.WebApi.Types.TrafficTypes.DevicesPerformanceDto'",
        "data": {
            "deviceData": paginated_data,
            "deviceFilterList": deviceFilterList,
            "rowsCount": len(deviceFilterList),
            "filteredRowsCount": total_count
        },
        "authenticationToken": None
    }


@router.get("/SearcherLookup")
def get_traffic_searcher_lookup(
    request: Request,
    RoadName: str      = Query("All", description="Filter by Road Name"),
    SuburbName: str    = Query("All", description="Filter by Suburb Name"),
    CityName: str      = Query("All", description="Filter by City Name"),
    DeviceIndex: str   = Query("All", description="Filter by Device Index"),
    SessionIndex: str  = Query("All", description="Filter by Session Index"),
    VehicleClass: str  = Query("All", description="Filter by Vehicle Class"),
    StartDate: str     = Query("All", description="Start Date (YYYY-MM-DD) or ALL"),
    EndDate: str       = Query("All", description="End Date (YYYY-MM-DD) or ALL")
):
    """
    Retrieves lookup lists from the flat 'traffic_datalookup' index,
    one document per (lookupType, lookupValue).
    """
    INDEX = "traffic_datalookup"
    MAX_TERMS = 10000

    def fetch_list(lookup_type: str, out_key: str):
        # Aggregate all lookupValue for the given lookupType
        body = {
            "size": 0,
            "query": {
                "term": { "lookupType.keyword": lookup_type }
            },
            "aggs": {
                "values": {
                    "terms": {
                        "field": "lookupValue.keyword",
                        "size": MAX_TERMS,
                        "order": {"_key": "asc"}
                    }
                }
            }
        }
        try:
            resp = es.search(index=INDEX, body=body)
        except Exception as e:
            logger.error(f"Error fetching {lookup_type} from ES: {e}")
            raise HTTPException(status_code=500, detail="Elasticsearch query failed")

        buckets = resp.get("aggregations", {}).get("values", {}).get("buckets", [])
        return [{ out_key: b["key"] } for b in buckets]

    data = {
        "roadNameList"    : fetch_list("Location0",   "roadName"),
        "suburbNameList"  : fetch_list("Location1",   "suburbName"),
        "cityNameList"    : fetch_list("Location2",   "cityName"),
        "deviceList"      : fetch_list("DeviceIndex", "deviceIndex"),
        "sessionList"     : fetch_list("SessionIndex","sessionIndex"),
        "vehicleClass": fetch_list("VehicleClass","vehicleClass"),
    }

    # Extract and validate Authorization token
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing Authorization header")

    token = auth_header.replace("Bearer ", "")
    try:
        user_data = decrypt_token_and_validate_roles(token)
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid token or insufficient permissions")
    except Exception as e:
        logger.error(f"Token decryption failed: {e}")
        raise HTTPException(status_code=401, detail="Token decryption failed")

    # Get username from user_data
    username = user_data.get("Name", "")
    if not username:
        raise HTTPException(status_code=401, detail="No username found in token")

    # Get user data from CSV using username
    csv_user_data = get_user_data_by_username(username)
    if not csv_user_data:
        raise HTTPException(status_code=401, detail="No user data found in CSV; please log in.")

    # Get token from CSV and validate it
    csv_token = csv_user_data.get("token")
    if not csv_token:
        raise HTTPException(status_code=401, detail="No token found in CSV for user")

    # Validate token using process_token
    validated_token = process_token(csv_token)

    return {
        "status": True,
        "message": "Item Found - 'VSB.AnalyticsApp.WebApi.Types.TrafficTypes.SearcherLookupDto'",
        "data": data,
        "authenticationToken": validated_token
    }

