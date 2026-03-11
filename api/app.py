"""
api/app.py
===========
RESTful API for the Logistics Data stored in MongoDB Atlas.

WHAT THIS API PROVIDES:
    A set of HTTP endpoints that allow operations teams, dashboards, and other
    services to query and analyze the delivery trip data that has been ingested
    via the Kafka pipeline.

ARCHITECTURAL DECISIONS:

    Framework Choice — FastAPI:
        We chose FastAPI over Flask for several reasons:
        1. Automatic OpenAPI/Swagger docs at /docs (great for showreel demos).
        2. Built-in request validation via Pydantic models.
        3. Async support for non-blocking MongoDB queries.
        4. Type hints provide self-documenting code.
        TRADE-OFF: FastAPI has a steeper initial learning curve than Flask,
        but the auto-generated docs and validation save significant time.

    Pagination:
        All list endpoints support skip/limit pagination. We default to limit=50
        to prevent accidental full-collection dumps. For the ~6,880 records in
        our dataset, this keeps response times under 100ms.

    Aggregation Pipelines:
        MongoDB's aggregation framework runs server-side, meaning we push
        computation to the database rather than pulling all data to the API
        and aggregating in Python. This is critical for performance at scale.

USE-CASES AND ASSUMPTIONS:
    1. Fleet Managers need to filter trips by vehicle, customer, or supplier.
    2. Operations teams need on-time delivery metrics (ontime='G' vs 'R').
    3. Route planners need distance and location-based queries.
    4. Customer success teams need trip history for specific customers.
    5. Data quality teams need access to dead-letter records for debugging.
    6. Dashboard services need aggregated statistics (avg distance, delay rates).
    7. We assume read-heavy workload — API consumers rarely write data.
    8. We assume the API is internal-facing (no public auth for the showreel).
    9. GPS coordinates in the data are India-centric (logistics in Tamil Nadu/Karnataka).
    10. The 'delay' field uses codes: 'R' for red (delayed), 'G' for green (on time).

USAGE:
    uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
"""

import sys
import os
from typing import Optional, List
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.config import MONGO_CONNECTION_STRING, MONGO_DATABASE, MONGO_COLLECTION, API_HOST, API_PORT

# =============================================================================
# App Initialization
# =============================================================================
app = FastAPI(
    title="Logistics Delivery Trip API",
    description=(
        "RESTful API for querying delivery trip truck data ingested from "
        "Confluent Kafka into MongoDB Atlas. Part of the Kafka-MongoDB "
        "Integration showreel project."
    ),
    version="1.0.0",
    docs_url="/docs",       # Swagger UI
    redoc_url="/redoc",     # ReDoc alternative
)

# CORS — allow all origins for demo. In production, restrict to specific domains.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# MongoDB Connection (reused across requests)
# =============================================================================
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]
dead_letter_collection = db[f'{MONGO_COLLECTION}_dead_letter']


# =============================================================================
# Pydantic Response Models (for API documentation and validation)
# =============================================================================
class HealthResponse(BaseModel):
    status: str
    database: str
    collection: str
    document_count: int


class TripSummary(BaseModel):
    total_trips: int
    unique_vehicles: int
    unique_customers: int
    unique_suppliers: int
    avg_distance_km: Optional[float]
    ontime_count: int
    delayed_count: int


class AggregationResult(BaseModel):
    _id: Optional[str] = None
    count: Optional[int] = None
    avg_distance: Optional[float] = None


# =============================================================================
# Helper function to make MongoDB docs JSON-serializable
# =============================================================================
def serialize_doc(doc: dict) -> dict:
    """Convert MongoDB document to JSON-serializable dict."""
    if doc is None:
        return {}
    doc['_id'] = str(doc.get('_id', ''))
    # Convert datetime objects to ISO strings
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


# =============================================================================
# ENDPOINT 1: Health Check
# =============================================================================
@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check():
    """
    Verify API connectivity to MongoDB and return basic stats.
    USE-CASE: Monitoring dashboards and Docker health checks.
    """
    try:
        count = collection.count_documents({})
        return HealthResponse(
            status="healthy",
            database=MONGO_DATABASE,
            collection=MONGO_COLLECTION,
            document_count=count
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {str(e)}")


# =============================================================================
# ENDPOINT 2: Get All Trips (paginated)
# =============================================================================
@app.get("/trips", tags=["Trips"])
def get_trips(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(50, ge=1, le=500, description="Max records to return"),
):
    """
    Retrieve delivery trips with pagination.
    USE-CASE: Displaying trip lists in a dashboard table with page controls.
    """
    cursor = collection.find({}).skip(skip).limit(limit)
    trips = [serialize_doc(doc) for doc in cursor]
    total = collection.count_documents({})
    return {
        "data": trips,
        "pagination": {"skip": skip, "limit": limit, "total": total}
    }


# =============================================================================
# ENDPOINT 3: Get Trip by BookingID
# =============================================================================
@app.get("/trips/booking/{booking_id:path}")
async def get_trips_by_booking(booking_id: str):
    """
    Retrieve all GPS records for a specific booking.
    USE-CASE: Tracking a specific shipment's journey, viewing its GPS trail.
    A single BookingID may have multiple records (one per GPS ping).
    """
    results = list(collection.find({"BookingID": booking_id}))
    if not results:
        raise HTTPException(status_code=404, detail=f"No trips found for BookingID: {booking_id}")
    return {"booking_id": booking_id, "records": [serialize_doc(r) for r in results]}


# =============================================================================
# ENDPOINT 4: Filter Trips by Vehicle
# =============================================================================
@app.get("/trips/vehicle/{vehicle_no}", tags=["Trips"])
def get_trips_by_vehicle(
    vehicle_no: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Retrieve all trips for a specific vehicle.
    USE-CASE: Fleet managers checking utilization and history for a truck.
    """
    query = {"vehicle_no": vehicle_no}
    results = list(collection.find(query).skip(skip).limit(limit))
    total = collection.count_documents(query)
    return {
        "vehicle_no": vehicle_no,
        "data": [serialize_doc(r) for r in results],
        "pagination": {"skip": skip, "limit": limit, "total": total}
    }


# =============================================================================
# ENDPOINT 5: Filter Trips by Customer
# =============================================================================
@app.get("/trips/customer/{customer_id}", tags=["Trips"])
def get_trips_by_customer(
    customer_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Retrieve all trips for a specific customer.
    USE-CASE: Customer success teams reviewing delivery history for a client.
    """
    query = {"customerID": customer_id}
    results = list(collection.find(query).skip(skip).limit(limit))
    total = collection.count_documents(query)
    return {
        "customer_id": customer_id,
        "data": [serialize_doc(r) for r in results],
        "pagination": {"skip": skip, "limit": limit, "total": total}
    }


# =============================================================================
# ENDPOINT 6: Filter by On-Time Status
# =============================================================================
@app.get("/trips/status/{status}", tags=["Trips"])
def get_trips_by_status(
    status: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Filter trips by on-time status: 'G' (green/on-time) or 'R' (red/delayed).
    USE-CASE: Operations team identifying problematic deliveries for root-cause analysis.
    """
    if status.upper() not in ('G', 'R'):
        raise HTTPException(status_code=400, detail="Status must be 'G' (on-time) or 'R' (delayed)")
    query = {"ontime": status.upper()}
    results = list(collection.find(query).skip(skip).limit(limit))
    total = collection.count_documents(query)
    return {
        "status": status.upper(),
        "status_label": "On Time" if status.upper() == 'G' else "Delayed",
        "data": [serialize_doc(r) for r in results],
        "pagination": {"skip": skip, "limit": limit, "total": total}
    }


# =============================================================================
# ENDPOINT 7: Search Trips (multi-field filter)
# =============================================================================
@app.get("/trips/search", tags=["Trips"])
def search_trips(
    gps_provider: Optional[str] = None,
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    material: Optional[str] = None,
    supplier: Optional[str] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Advanced search with multiple optional filters.
    USE-CASE: Analysts building custom reports, e.g., "Show me all trips by VAMOSYS
    GPS from Chennai to Hosur carrying bracket materials over 200km."
    
    ASSUMPTION: Text searches use regex for partial matching (case-insensitive).
    This is acceptable for our dataset size. For millions of records, you would
    use MongoDB Atlas Search (full-text search) instead.
    """
    query = {}
    if gps_provider:
        query["GpsProvider"] = {"$regex": gps_provider, "$options": "i"}
    if origin:
        query["Origin_Location"] = {"$regex": origin, "$options": "i"}
    if destination:
        query["Destination_Location"] = {"$regex": destination, "$options": "i"}
    if material:
        query["Material_Shipped"] = {"$regex": material, "$options": "i"}
    if supplier:
        query["supplierNameCode"] = {"$regex": supplier, "$options": "i"}
    if min_distance is not None or max_distance is not None:
        dist_filter = {}
        if min_distance is not None:
            dist_filter["$gte"] = min_distance
        if max_distance is not None:
            dist_filter["$lte"] = max_distance
        query["TRANSPORTATION_DISTANCE_IN_KM"] = dist_filter

    results = list(collection.find(query).skip(skip).limit(limit))
    total = collection.count_documents(query)
    return {
        "filters_applied": {k: v for k, v in query.items()},
        "data": [serialize_doc(r) for r in results],
        "pagination": {"skip": skip, "limit": limit, "total": total}
    }


# =============================================================================
# ENDPOINT 8: Summary Statistics
# =============================================================================
@app.get("/analytics/summary", response_model=TripSummary, tags=["Analytics"])
def get_summary():
    """
    Get high-level summary statistics of the entire dataset.
    USE-CASE: Executive dashboard showing KPIs at a glance.
    """
    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_trips": {"$sum": 1},
                "unique_vehicles": {"$addToSet": "$vehicle_no"},
                "unique_customers": {"$addToSet": "$customerID"},
                "unique_suppliers": {"$addToSet": "$supplierID"},
                "avg_distance": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
                "ontime_count": {
                    "$sum": {"$cond": [{"$eq": ["$ontime", "G"]}, 1, 0]}
                },
                "delayed_count": {
                    "$sum": {"$cond": [{"$eq": ["$ontime", "R"]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "total_trips": 1,
                "unique_vehicles": {"$size": "$unique_vehicles"},
                "unique_customers": {"$size": "$unique_customers"},
                "unique_suppliers": {"$size": "$unique_suppliers"},
                "avg_distance_km": {"$round": ["$avg_distance", 2]},
                "ontime_count": 1,
                "delayed_count": 1,
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    if not results:
        return TripSummary(
            total_trips=0, unique_vehicles=0, unique_customers=0,
            unique_suppliers=0, avg_distance_km=0.0, ontime_count=0, delayed_count=0
        )
    return results[0]


# =============================================================================
# ENDPOINT 9: Aggregation — Trips per GPS Provider
# =============================================================================
@app.get("/analytics/by-gps-provider", tags=["Analytics"])
def trips_by_gps_provider():
    """
    Count trips and average distance grouped by GPS provider.
    USE-CASE: Evaluating GPS provider reliability and coverage.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$GpsProvider",
                "trip_count": {"$sum": 1},
                "avg_distance_km": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
            }
        },
        {"$sort": {"trip_count": -1}},
        {
            "$project": {
                "gps_provider": "$_id",
                "trip_count": 1,
                "avg_distance_km": {"$round": ["$avg_distance_km", 2]},
                "_id": 0,
            }
        }
    ]
    return {"data": list(collection.aggregate(pipeline))}


# =============================================================================
# ENDPOINT 10: Aggregation — Trips per Customer
# =============================================================================
@app.get("/analytics/by-customer", tags=["Analytics"])
def trips_by_customer(top_n: int = Query(10, ge=1, le=100)):
    """
    Top N customers by trip count with average distance.
    USE-CASE: Identifying key accounts and their logistics volume.
    """
    pipeline = [
        {"$match": {"customerID": {"$ne": None}}},
        {
            "$group": {
                "_id": {"customerID": "$customerID", "customerName": "$customerNameCode"},
                "trip_count": {"$sum": 1},
                "avg_distance_km": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
            }
        },
        {"$sort": {"trip_count": -1}},
        {"$limit": top_n},
        {
            "$project": {
                "customer_id": "$_id.customerID",
                "customer_name": "$_id.customerName",
                "trip_count": 1,
                "avg_distance_km": {"$round": ["$avg_distance_km", 2]},
                "_id": 0,
            }
        }
    ]
    return {"data": list(collection.aggregate(pipeline))}


# =============================================================================
# ENDPOINT 11: Aggregation — On-Time Delivery Rate by Supplier
# =============================================================================
@app.get("/analytics/supplier-performance", tags=["Analytics"])
def supplier_performance():
    """
    On-time delivery rate and trip count per supplier.
    USE-CASE: Vendor performance evaluation for contract negotiations.
    
    ASSUMPTION: Only records with non-null ontime values are included in
    the calculation. Records with null ontime are excluded to avoid
    skewing the percentages.
    """
    pipeline = [
        {"$match": {"ontime": {"$in": ["G", "R"]}}},
        {
            "$group": {
                "_id": {"supplierID": "$supplierID", "supplierName": "$supplierNameCode"},
                "total_trips": {"$sum": 1},
                "ontime_trips": {
                    "$sum": {"$cond": [{"$eq": ["$ontime", "G"]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "supplier_id": "$_id.supplierID",
                "supplier_name": "$_id.supplierName",
                "total_trips": 1,
                "ontime_trips": 1,
                "ontime_rate_pct": {
                    "$round": [
                        {"$multiply": [{"$divide": ["$ontime_trips", "$total_trips"]}, 100]},
                        2
                    ]
                },
                "_id": 0,
            }
        },
        {"$sort": {"total_trips": -1}},
    ]
    return {"data": list(collection.aggregate(pipeline))}


# =============================================================================
# ENDPOINT 12: Aggregation — Route Analysis (Top Routes)
# =============================================================================
@app.get("/analytics/top-routes", tags=["Analytics"])
def top_routes(top_n: int = Query(10, ge=1, le=50)):
    """
    Most common origin-destination routes with average distance and delay stats.
    USE-CASE: Route optimization and capacity planning.
    """
    pipeline = [
        {
            "$group": {
                "_id": {
                    "origin": "$Origin_Location",
                    "destination": "$Destination_Location"
                },
                "trip_count": {"$sum": 1},
                "avg_distance_km": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
                "delayed_count": {
                    "$sum": {"$cond": [{"$eq": ["$ontime", "R"]}, 1, 0]}
                },
            }
        },
        {"$sort": {"trip_count": -1}},
        {"$limit": top_n},
        {
            "$project": {
                "origin": "$_id.origin",
                "destination": "$_id.destination",
                "trip_count": 1,
                "avg_distance_km": {"$round": ["$avg_distance_km", 2]},
                "delayed_count": 1,
                "_id": 0,
            }
        }
    ]
    return {"data": list(collection.aggregate(pipeline))}


# =============================================================================
# ENDPOINT 13: Dead-Letter Records (Data Quality)
# =============================================================================
@app.get("/data-quality/dead-letters", tags=["Data Quality"])
def get_dead_letters(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Retrieve records that failed validation and were sent to the dead-letter collection.
    USE-CASE: Data engineering team investigating and fixing data quality issues.
    """
    results = list(dead_letter_collection.find({}).skip(skip).limit(limit))
    total = dead_letter_collection.count_documents({})
    return {
        "data": [serialize_doc(r) for r in results],
        "pagination": {"skip": skip, "limit": limit, "total": total}
    }


# =============================================================================
# ENDPOINT 14: Distance Distribution
# =============================================================================
@app.get("/analytics/distance-distribution", tags=["Analytics"])
def distance_distribution():
    """
    Distribution of trips by distance buckets (0-100km, 100-300km, 300-500km, 500+km).
    USE-CASE: Understanding the logistics network's distance profile for fleet sizing.
    """
    pipeline = [
        {"$match": {"TRANSPORTATION_DISTANCE_IN_KM": {"$ne": None}}},
        {
            "$bucket": {
                "groupBy": "$TRANSPORTATION_DISTANCE_IN_KM",
                "boundaries": [0, 100, 300, 500, 1000, 5000],
                "default": "5000+",
                "output": {
                    "count": {"$sum": 1},
                    "avg_distance": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
                }
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    # Format bucket labels
    labels = {0: "0-100 km", 100: "100-300 km", 300: "300-500 km", 500: "500-1000 km", 1000: "1000-5000 km"}
    formatted = []
    for r in results:
        bucket_id = r['_id']
        formatted.append({
            "range": labels.get(bucket_id, f"{bucket_id}+ km"),
            "count": r['count'],
            "avg_distance_km": round(r.get('avg_distance', 0), 2),
        })
    return {"data": formatted}


# =============================================================================
# Run with uvicorn
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.app:app", host=API_HOST, port=API_PORT, reload=True)
