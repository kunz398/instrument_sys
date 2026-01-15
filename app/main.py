import sys
from pathlib import Path

# Add /app to Python path (since Docker copies files to /app)
sys.path.append(str(Path(__file__).parent))
from typing import Union
from fastapi import FastAPI, Request, Response, APIRouter, Depends, HTTPException, Header, status, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse, StreamingResponse,FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import io, os
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_
from app.db import AsyncSessionLocal
from sqlalchemy import select
from sqlalchemy.orm import selectinload
import os
import shutil
from uuid import uuid4
from typing import List, Optional
from sqlalchemy import asc, desc
from app.models import Type, Status, AccessMethod, Station, Token
from app.schemas import (
    TypeCreate,
    TypeOut,
    StatusCreate,
    StatusOut,
    AccessMethodCreate,
    AccessMethodOut,
    StationCreate,
    StationOut,
    TokenCreate,
    TokenOut,
    BadDataRequest,
    ChartTypeUpdate,
)
from app.methods import *
import httpx
from sqlalchemy import select, outerjoin

API_TOKEN = "99a920305541f1c38db611ebab95ba"


async def verify_token(x_token: str = Header(...)):
    if x_token != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing token"
        )


app = FastAPI(
    docs_url="/insitu/docs",
    redoc_url="/insitu/redoc",
    openapi_url="/insitu/openapi.json",
    favicon_url="/insitu/favicon.ico"
)
# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (for development)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Dependency
async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

# HTTP Client dependency
async def get_http_client() -> httpx.AsyncClient:
    async with httpx.AsyncClient(verify=False) as client:
        yield client

ocean_router = APIRouter(prefix="/insitu")

get_data_router = APIRouter(prefix="/insitu/get_data")

@ocean_router.get("/")
def read_root():
    return {"Message": "Welcome to Instrument Management System, powered by OpenAPI"}

# Favicon route (must include the prefix)
@app.get("/favicon.ico", include_in_schema=False)
@ocean_router.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    # Get the absolute path to the favicon
    favicon_path = Path(__file__).parent / "icon.ico"
    if not favicon_path.exists():
        raise HTTPException(status_code=404, detail="Favicon not found")
    return FileResponse(favicon_path)


## DOCUMENT TYPES
@ocean_router.get("/types/", response_model=List[TypeOut])
async def get_document_types(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Type))
    return result.scalars().all()

# CREATE Type
@ocean_router.post("/types/", response_model=TypeOut, dependencies=[Depends(verify_token)])
async def create_type(type_data: TypeCreate, db: AsyncSession = Depends(get_db)):
    type_obj = Type(**type_data.dict())
    db.add(type_obj)
    try:
        await db.commit()
        await db.refresh(type_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Type with this value already exists.")
    return type_obj

# UPDATE Type
@ocean_router.put("/types/{type_id}", response_model=TypeOut, dependencies=[Depends(verify_token)])
async def update_type(type_id: int, type_data: TypeCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Type).where(Type.id == type_id))
    type_obj = result.scalar_one_or_none()
    if not type_obj:
        raise HTTPException(status_code=404, detail="Type not found")
    for key, value in type_data.dict().items():
        setattr(type_obj, key, value)
    try:
        await db.commit()
        await db.refresh(type_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Type with this value already exists.")
    return type_obj

# DELETE Type
@ocean_router.delete("/types/{type_id}", status_code=204, dependencies=[Depends(verify_token)])
async def delete_type(type_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Type).where(Type.id == type_id))
    type_obj = result.scalar_one_or_none()
    if not type_obj:
        raise HTTPException(status_code=404, detail="Type not found")
    await db.delete(type_obj)
    await db.commit()
    return Response(status_code=204)

# LIST ALL Status
@ocean_router.get("/status/", response_model=List[StatusOut])
async def get_statuses(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Status))
    return result.scalars().all()

# CREATE Status
@ocean_router.post("/status/", response_model=StatusOut, dependencies=[Depends(verify_token)])
async def create_status(status_data: StatusCreate, db: AsyncSession = Depends(get_db)):
    status_obj = Status(**status_data.dict())
    db.add(status_obj)
    try:
        await db.commit()
        await db.refresh(status_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Status with this value already exists.")
    return status_obj

# UPDATE Status
@ocean_router.put("/status/{status_id}", response_model=StatusOut, dependencies=[Depends(verify_token)])
async def update_status(status_id: int, status_data: StatusCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Status).where(Status.id == status_id))
    status_obj = result.scalar_one_or_none()
    if not status_obj:
        raise HTTPException(status_code=404, detail="Status not found")
    for key, value in status_data.dict().items():
        setattr(status_obj, key, value)
    try:
        await db.commit()
        await db.refresh(status_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Status with this value already exists.")
    return status_obj

# DELETE Status
@ocean_router.delete("/status/{status_id}", status_code=204, dependencies=[Depends(verify_token)])
async def delete_status(status_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Status).where(Status.id == status_id))
    status_obj = result.scalar_one_or_none()
    if not status_obj:
        raise HTTPException(status_code=404, detail="Status not found")
    await db.delete(status_obj)
    await db.commit()
    return Response(status_code=204)


# LIST ALL AccessMethods
@ocean_router.get("/access_methods/", response_model=List[AccessMethodOut])
async def get_access_methods(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AccessMethod))
    return result.scalars().all()

# CREATE AccessMethod
@ocean_router.post(
    "/access_methods/",
    response_model=AccessMethodOut,
    dependencies=[Depends(verify_token)]
)
async def create_access_method(
    access_method_data: AccessMethodCreate, db: AsyncSession = Depends(get_db)
):
    access_method = AccessMethod(**access_method_data.dict())
    db.add(access_method)
    try:
        await db.commit()
        await db.refresh(access_method)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="AccessMethod with this function already exists.")
    return access_method

# UPDATE AccessMethod
@ocean_router.put(
    "/access_methods/{access_method_id}",
    response_model=AccessMethodOut,
    dependencies=[Depends(verify_token)]
)
async def update_access_method(
    access_method_id: int, access_method_data: AccessMethodCreate, db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(AccessMethod).where(AccessMethod.id == access_method_id))
    access_method = result.scalar_one_or_none()
    if not access_method:
        raise HTTPException(status_code=404, detail="AccessMethod not found")
    for key, value in access_method_data.dict().items():
        setattr(access_method, key, value)
    try:
        await db.commit()
        await db.refresh(access_method)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="AccessMethod with this function already exists.")
    return access_method

# DELETE AccessMethod
@ocean_router.delete(
    "/access_methods/{access_method_id}",
    status_code=204,
    dependencies=[Depends(verify_token)]
)
async def delete_access_method(
    access_method_id: int, db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(AccessMethod).where(AccessMethod.id == access_method_id))
    access_method = result.scalar_one_or_none()
    if not access_method:
        raise HTTPException(status_code=404, detail="AccessMethod not found")
    await db.delete(access_method)
    await db.commit()
    return Response(status_code=204)


# ---------- TOKEN ENDPOINTS ----------
# LIST ALL Tokens
@ocean_router.get("/tokens/", response_model=List[TokenOut])
async def get_tokens(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Token))
    return result.scalars().all()

# GET Token by ID
@ocean_router.get("/tokens/{token_id}", response_model=TokenOut)
async def get_token_by_id(token_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Token).where(Token.id == token_id))
    token_obj = result.scalar_one_or_none()
    if not token_obj:
        raise HTTPException(status_code=404, detail="Token not found")
    return token_obj

# CREATE Token
@ocean_router.post("/tokens/", response_model=TokenOut, dependencies=[Depends(verify_token)])
async def create_token(token_data: TokenCreate, db: AsyncSession = Depends(get_db)):
    token_obj = Token(**token_data.dict())
    db.add(token_obj)
    try:
        await db.commit()
        await db.refresh(token_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Token with this value already exists.")
    return token_obj

# UPDATE Token
@ocean_router.put("/tokens/{token_id}", response_model=TokenOut, dependencies=[Depends(verify_token)])
async def update_token(token_id: int, token_data: TokenCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Token).where(Token.id == token_id))
    token_obj = result.scalar_one_or_none()
    if not token_obj:
        raise HTTPException(status_code=404, detail="Token not found")
    for key, value in token_data.dict().items():
        setattr(token_obj, key, value)
    try:
        await db.commit()
        await db.refresh(token_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Token with this value already exists.")
    return token_obj

# DELETE Token
@ocean_router.delete("/tokens/{token_id}", status_code=204, dependencies=[Depends(verify_token)])
async def delete_token(token_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Token).where(Token.id == token_id))
    token_obj = result.scalar_one_or_none()
    if not token_obj:
        raise HTTPException(status_code=404, detail="Token not found")
    await db.delete(token_obj)
    await db.commit()
    return Response(status_code=204)


# LIST ALL Stations
@ocean_router.get("/stations/", response_model=List[dict])
async def get_stations(
    type_id: Optional[int] = Query(None, description="Filter by type ID"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    access_method_id: Optional[int] = Query(None, description="Filter by access method ID"),
    project: Optional[str] = Query(None, description="Filter by project name"),
    type_value: Optional[str] = Query(None, description="Filter by type value"),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(Station, Type.value.label("type_value"))
        .outerjoin(Type, Station.type_id == Type.id)
        .where(Station.status_id == 1)
    )
    
    # Apply optional filters
    if type_id is not None:
        stmt = stmt.where(Station.type_id == type_id)
    if country_id is not None:
        stmt = stmt.where(Station.country_id == country_id)
    if access_method_id is not None:
        stmt = stmt.where(Station.access_method_id == access_method_id)
    if project is not None:
        stmt = stmt.where(Station.project == project)
    if type_value is not None:
        stmt = stmt.where(Type.value == type_value)
    
    result = await db.execute(stmt)
    stations = []
    for station, type_value in result.all():
        station_dict = station.__dict__.copy()
        station_dict.pop("_sa_instance_state", None)
        station_dict["type_value"] = type_value
        # Set owner to 'unknown' if None, empty, or 'NULL'
        owner = station_dict.get("owner")
        if not owner or (isinstance(owner, str) and owner.strip().lower() == "null"):
            station_dict["owner"] = "SPC"
        # Ensure bad_data is included (it should already be there from __dict__)
        station_dict["bad_data"] = station.bad_data
        # # sep
        station_dict["station_id"] = station.station_id
        station_dict["display_name"] = station.display_name
        stations.append(station_dict)
    return stations

# GET Station by ID or station_id
@ocean_router.get("/stations/{station_id}", response_model=StationOut)
async def get_station_by_id(station_id: str, db: AsyncSession = Depends(get_db)):
    # Find the status_id for 'active'
    status_result = await db.execute(select(Status).where(Status.value == 'active'))
    active_status = status_result.scalar_one_or_none()
    if not active_status:
        raise HTTPException(status_code=404, detail="Status 'active' not found")
    active_status_id = active_status.id

    # Try to convert to int for internal ID search
    try:
        internal_id = int(station_id)
        # First try to find by internal ID
        result = await db.execute(
            select(Station).where(
                Station.id == internal_id,
                Station.status_id == active_status_id
            )
        )
        station_obj = result.scalar_one_or_none()
        
        # If not found by internal ID, try by station_id
        if not station_obj:
            result = await db.execute(
                select(Station).where(
                    Station.station_id == station_id,
                    Station.status_id == active_status_id
                )
            )
            station_obj = result.scalar_one_or_none()
    except ValueError:
        # If not an integer, search only by station_id
        result = await db.execute(
            select(Station).where(
                Station.station_id == station_id,
                Station.status_id == active_status_id
            )
        )
        station_obj = result.scalar_one_or_none()
    if not station_obj:
        raise HTTPException(status_code=404, detail=f"Active station not found with id or station_id: {station_id}")
    return station_obj

# CREATE Station
@ocean_router.post("/stations/", response_model=StationOut, dependencies=[Depends(verify_token)])
async def create_station(station_data: StationCreate, db: AsyncSession = Depends(get_db)):
    station_obj = Station(**station_data.dict())
    db.add(station_obj)
    try:
        await db.commit()
        await db.refresh(station_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Station could not be created.")
    return station_obj

# UPDATE Station by id or station_id
@ocean_router.put("/stations/{identifier}", response_model=StationOut, dependencies=[Depends(verify_token)])
async def update_station_by_identifier(identifier: str, station_data: StationCreate, db: AsyncSession = Depends(get_db)):
    # Try to convert identifier to int for internal id search
    try:
        internal_id = int(identifier)
        result = await db.execute(select(Station).where(Station.id == internal_id))
    except ValueError:
        # If not an integer, search by station_id
        result = await db.execute(select(Station).where(Station.station_id == identifier))
    station_obj = result.scalar_one_or_none()
    if not station_obj:
        raise HTTPException(status_code=404, detail=f"Station not found with id or station_id: {identifier}")
    # Update only the fields that are provided (exclude station_id from updates if updating by station_id)
    update_data = station_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(station_obj, key, value)
    try:
        await db.commit()
        await db.refresh(station_obj)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="Station could not be updated.")
    return station_obj

# DELETE Station
@ocean_router.delete("/stations/{station_id}", response_model=StationOut, dependencies=[Depends(verify_token)])
async def delete_station(station_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Station).where(Station.id == station_id))
    station_obj = result.scalar_one_or_none()
    if not station_obj:
        raise HTTPException(status_code=404, detail="Station not found")
    # Find the status with value 'deleted'
    status_result = await db.execute(select(Status).where(Status.value == 'deleted'))
    deleted_status = status_result.scalar_one_or_none()
    if not deleted_status:
        raise HTTPException(status_code=404, detail="Status 'deleted' not found")
    station_obj.status_id = deleted_status.id
    await db.commit()
    await db.refresh(station_obj)
    return station_obj


# UPDATE chart_type for a station by internal id or station_id
@ocean_router.put(
    "/stations/{identifier}/chart_type",
    response_model=StationOut,
    dependencies=[Depends(verify_token)]
)
async def update_station_chart_type(
    identifier: str,
    payload: ChartTypeUpdate,
    db: AsyncSession = Depends(get_db)
):
    station_obj = None

    # Try to find by internal numeric ID 
    try:
        internal_id = int(identifier)
        result = await db.execute(select(Station).where(Station.id == internal_id))
        station_obj = result.scalar_one_or_none()
    except ValueError:
        station_obj = None

    # # If not found (or identifier wasn't numeric), fall back to station_id string
    # if not station_obj:
    #     result = await db.execute(select(Station).where(Station.station_id == identifier))
    #     station_obj = result.scalar_one_or_none()

    if not station_obj:
        raise HTTPException(status_code=404, detail=f"Station not found with id or station_id: {identifier}")

    station_obj.chart_type = payload.chart_type

    await db.commit()
    await db.refresh(station_obj)
    return station_obj


# ---------- BAD DATA ENDPOINTS ----------

# PUT bad_data with JSON body
@ocean_router.put("/bad_data/", dependencies=[Depends(verify_token)])
async def update_bad_data(
    request: BadDataRequest,
    db: AsyncSession = Depends(get_db)
):
    station_id = request.station_id
    bad_data = request.bad_data
    
    # Try to convert to int for internal ID search
    try:
        internal_id = int(station_id)
        # First try to find by internal ID
        result = await db.execute(
            select(Station).where(Station.id == internal_id)
        )
        station_obj = result.scalar_one_or_none()
        if not station_obj:
            # If not found by internal ID, try by station_id
            result = await db.execute(
                select(Station).where(Station.station_id == station_id)
            )
            station_obj = result.scalar_one_or_none()
    except ValueError:
        # If not an integer, search by station_id
        result = await db.execute(
            select(Station).where(Station.station_id == station_id)
        )
        station_obj = result.scalar_one_or_none()
    if not station_obj:
        raise HTTPException(status_code=404, detail=f"Station not found with id or station_id: {station_id}")
    
    # Update the bad_data field (allows empty strings)
    station_obj.bad_data = bad_data
    await db.commit()
    await db.refresh(station_obj)
    
    return {
        "id": station_obj.id,
        "station_id": station_obj.station_id,
        "display_name": station_obj.display_name,
        "station_description": station_obj.description,
        "bad_data": station_obj.bad_data,
        "mean": station_obj.mean,
        "message": "Bad data updated successfully"
    }

'''This section is for the get_data endpoints.  '''
# GET DATA for specific station
@get_data_router.get("/station/{station_id}")
async def get_station_data(
    station_id: str,
    limit: int = Query(100, ge=1),  # default 100, min 1, max limit removed (was le=1000)
    start: str = Query(None, description="Start datetime in ISO format, e.g. 2025-08-01T00:00:00Z"),
    end: str = Query(None, description="End datetime in ISO format, e.g. 2025-08-21T23:59:59Z"),
    db: AsyncSession = Depends(get_db),
    http_client: httpx.AsyncClient = Depends(get_http_client)
):
    # Find the status_id for 'active'
    status_result = await db.execute(select(Status).where(Status.value == 'active'))
    active_status = status_result.scalar_one_or_none()
    if not active_status:
        raise HTTPException(status_code=404, detail="Status 'active' not found")
    active_status_id = active_status.id

    # Search only by station_id field
    result = await db.execute(
        select(Station).options(selectinload(Station.types)).where(
            Station.station_id == station_id,
            Station.status_id == active_status_id
        )
    )
    station = result.scalar_one_or_none()
    
    # # Try to convert to int for internal ID search
    # try:
    #     internal_id = int(station_id)
    #     # First try to find by internal ID
    #     result = await db.execute(
    #         select(Station).options(selectinload(Station.types)).where(
    #             Station.id == internal_id,
    #             Station.status_id == active_status_id
    #         )
    #     )
    #     station = result.scalar_one_or_none()
    #     
    #     # If not found by internal ID, try by station_id
    #     if not station:
    #         result = await db.execute(
    #             select(Station).options(selectinload(Station.types)).where(
    #                 Station.station_id == station_id,
    #                 Station.status_id == active_status_id
    #             )
    #         )
    #         station = result.scalar_one_or_none()
    # except ValueError:
    #     # If not an integer, search only by station_id
    #     result = await db.execute(
    #         select(Station).options(selectinload(Station.types)).where(
    #             Station.station_id == station_id,
    #             Station.status_id == active_status_id
    #         )
    #     )
    #     station = result.scalar_one_or_none()
    
    if not station:
        raise HTTPException(status_code=404, detail="Active station not found")
    
    if not station.access_method_id:
        raise HTTPException(status_code=400, detail="Station has no access method configured")
    
    # Get the access method function name
    access_result = await db.execute(
        select(AccessMethod).where(AccessMethod.id == station.access_method_id)
    )
    access_method = access_result.scalar_one_or_none()
    
    if not access_method:
        raise HTTPException(status_code=404, detail="Access method not found")
    
    function_name = access_method.function
    
    # Check if the function exists in our method mapping
    if function_name not in METHOD_MAPPING:
        raise HTTPException(
            status_code=400,
            detail=f"Function '{function_name}' not implemented in methods.py"
        )
    # Execute the function dynamically
    try:
        function = METHOD_MAPPING[function_name]
        result_data = await function(station, limit=limit, start=start, end=end)
        # Get the type value from the related Type model
        type_value = station.types.value if station.types else None
        return {
            "id": station.id,
            "station_id": station.station_id,
            "display_name": station.display_name,
            "station_description": station.description,
            "type": type_value,
            "longitude": station.longitude,
            "latitude": station.latitude,
            "data_labels": station.variable_label,
            "bad_data": station.bad_data,
            "mean": station.mean,
            "chart_type": station.chart_type,
            "data_limit": station.data_limit,
            "data": result_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error executing function '{function_name}': {str(e)}"
        )



app.include_router(ocean_router)
app.include_router(get_data_router)