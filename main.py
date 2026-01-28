# main.py
import os
import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional, Literal

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from pudu_client import PuduClient

# Load .env in development
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Settings:
    """
    Application settings loaded from environment variables.
    """

    def __init__(self) -> None:
        self.pudu_api_key = os.getenv("APP_KEY", "")
        self.pudu_api_secret = os.getenv("APP_SECRET", "")
        self.pudu_base_url = os.getenv(
            "PUDU_BASE_URL",
            "https://open-platform.pudutech.com/pudu-entry",  # override in .env
        )


@lru_cache()
def get_settings() -> Settings:
    return Settings()


@lru_cache()
def get_pudu_client(settings: Settings = Depends(get_settings)) -> PuduClient:
    """
    Singleton PuduClient, created once per process.
    """
    try:
        return PuduClient(
            api_key=settings.pudu_api_key,
            api_secret=settings.pudu_api_secret,
            base_url=settings.pudu_base_url,
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Shared Swagger helpers ---------- #

LANGUAGE_HEADER_DESC = (
    "Optional response language. If omitted, Pudu defaults to Chinese.\n\n"
    "Header name must be exactly `Language`.\n"
    "Supported values depend on Pudu tenant; see Pudu appendix."
)


# ---------- Pydantic request models for Swagger ---------- #

class StoreMapListResponseItem(BaseModel):
    map_name: str = Field(..., description="Map name under the shop.")


class DeliveryMapInfo(BaseModel):
    map_name: str = Field(
        ...,
        description="Map name that the destination point belongs to.",
        example="Restaurant-1F",
    )


class DeliveryDestination(BaseModel):
    destination: str = Field(
        ...,
        description='Delivery destination point name (formerly called "points").',
        example="TABLE-001",
    )
    id: Optional[str] = Field(
        None,
        description=(
            "Order / business id. Pudu docs say it can be omitted, but may also be required "
            "depending on the robot / tenant. If you have a business order id, put it here."
        ),
        example="DELIVERY-001",
    )
    phone_num: Optional[str] = Field(
        None,
        description="Phone number (optional).",
        example="13800138000",
    )
    phone_code: Optional[str] = Field(
        None,
        description="Phone country/area code (optional).",
        example="+86",
    )
    map_info: Optional[DeliveryMapInfo] = Field(
        None,
        description="Optional map context if needed (map_name).",
    )


class DeliveryTray(BaseModel):
    destinations: List[DeliveryDestination] = Field(
        ...,
        description="List of delivery targets for this tray.",
        min_items=1,
    )


DeliveryType = Literal["NEW", "MODIFY"]
DeliverySort = Literal["AUTO", "FIXED"]


class DeliveryTaskPayload(BaseModel):
    type: DeliveryType = Field(
        ...,
        description=(
            "Task mode:\n"
            "- NEW: create a new delivery task (shows on edit page)\n"
            "- MODIFY: forcibly modify the current delivery task while robot is executing one"
        ),
        example="NEW",
    )
    delivery_sort: DeliverySort = Field(
        ...,
        description=(
            "Destination ordering:\n"
            "- AUTO: robot sorts by nearest\n"
            "- FIXED: keep the order you provide"
        ),
        example="AUTO",
    )
    execute_task: bool = Field(
        ...,
        description=(
            "Whether the robot should execute immediately:\n"
            "- true: execute directly after receiving\n"
            "- false: task is created but must be started manually on robot UI "
            'or via DeliveryAction action="START".'
        ),
        example=True,
    )
    trays: List[DeliveryTray] = Field(
        ...,
        description="Trays list (multi-tray delivery).",
        min_items=1,
    )


class DeliveryTaskBody(BaseModel):
    sn: str = Field(..., description="Robot serial number.", example="SN-PD202405000001")
    payload: DeliveryTaskPayload = Field(..., description="Delivery task parameters.")


DeliveryActionEnum = Literal[
    "START",
    "COMPLETE",
    "CANCEL_ALL_DELIVERY",
    "PAUSE",
    "RESUME",
]


class DeliveryActionPayload(BaseModel):
    action: DeliveryActionEnum = Field(
        ...,
        description=(
            "Delivery operation command:\n"
            "- START: start the delivery task (effective on delivery task interface)\n"
            "- COMPLETE: complete current delivery (only effective when arrival screen is displayed)\n"
            "- CANCEL_ALL_DELIVERY: cancel all delivery tasks (only valid during execution)\n"
            "- PAUSE: pause current delivery task (currently only supported by T300)\n"
            "- RESUME: resume a paused delivery task (currently only supported by T300)"
        ),
        example="START",
    )


class DeliveryActionBody(BaseModel):
    sn: str = Field(..., description="Robot serial number.", example="SN-PD202405000001")
    payload: DeliveryActionPayload = Field(..., description="Operation command parameters.")


class TaskErrandBody(BaseModel):
    sn: str = Field(..., description="Robot serial number")
    payload: Dict[str, Any] = Field(
        ...,
        description=(
            "Errand payload. Must contain a `tasks` array with `task_name`, "
            "`task_desc` and `point_list` (map points) entries."
        ),
    )


class TransportTaskBody(BaseModel):
    sn: str = Field(..., description="Robot serial number")
    payload: Dict[str, Any] = Field(
        ...,
        description=(
            "Transport task payload. Shape depends on robot model "
            "(start_point, destinations, trays, etc.)."
        ),
    )


class PositionCommandPayload(BaseModel):
    interval: int = Field(
        ...,
        ge=1,
        description="Intervalle entre deux positions (secondes), min 1",
        example=5,
    )
    times: int = Field(
        ...,
        ge=1,
        le=1000,
        description="Nombre de positions à envoyer (max 1000)",
        example=100,
    )
    source: str = Field(
        default="openAPI",
        description="Identifiant de la source (optionnel, ex: 'openAPI')",
        example="openAPI",
    )


class PositionCommandBody(BaseModel):
    sn: str = Field(..., description="Robot serial number")
    payload: PositionCommandPayload = Field(
        ...,
        description="Paramètres pour le reporting de position",
    )


class CustomCallBody(BaseModel):
    sn: Optional[str] = Field(
        None,
        description=(
            "Robot serial number (or use shop_id at Pudu level if "
            "your tenant allows)."
        ),
    )
    map_name: str = Field(..., description="Map name where the target point is.")
    point: str = Field(..., description="Map point name to call the robot to.")
    call_device_name: str = Field(
        ...,
        description="Identifier of this calling system (e.g. 'TRASH_ROUTE_BACKEND').",
    )
    # allow any extra fields like call_mode, mode_data, etc.
    extra: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional fields (call_mode, mode_data, shop_id, etc.).",
    )


class CustomCallCancelBody(BaseModel):
    task_id: str = Field(..., description="ID of the call task to cancel.")
    call_device_name: str = Field(..., description="Identifier of the calling system.")


class CustomCallCompleteBody(BaseModel):
    task_id: str = Field(..., description="ID of the call task to complete.")
    call_device_name: str = Field(..., description="Identifier of the calling system.")
    next_call_task: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional next call task definition to chain another call.",
    )


class GenericBody(BaseModel):
    """Generic 'pass-through' body for Pudu endpoints we don't want to strongly type yet."""
    data: Dict[str, Any]


# ---------- Callback models (notifyRobotPose) ---------- #

class RobotPoseData(BaseModel):
    x: float = Field(..., description="X coordinate")
    y: float = Field(..., description="Y coordinate")
    yaw: float = Field(..., description="Angle (yaw)")
    sn: str = Field(..., description="Robot SN")
    mac: str = Field(..., description="Robot MAC address")
    timestamp: int = Field(
        ...,
        description="Current timestamp (seconds, côté cloud).",
    )
    notify_timestamp: int = Field(
        ...,
        description="Timestamp reporté par le robot (millisecondes).",
    )


class RobotPoseCallback(BaseModel):
    callback_type: str = Field(
        ...,
        description="Callback message type, doit être 'notifyRobotPose'.",
    )
    data: RobotPoseData


# ---------- FastAPI App ---------- #

app = FastAPI(
    title="Pudu Cloud – API Wrapper",
    version="1.1.0",
    description=(
        "Thin FastAPI wrapper around Pudu Cloud for the trash cleaning / BellaBot use case.\n\n"
        "All `/pudu/...` endpoints proxy to Pudu Cloud with proper HMAC-SHA1 signing.\n"
        "Use Swagger UI (`/docs`) to quickly test Pudu operations with your robot SNs, "
        "group IDs and map point names."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """
    Close the underlying HTTP client when the app stops.
    """
    client = get_pudu_client(get_settings())
    await client.close()


# ---------- Local health endpoint ---------- #

@app.get(
    "/healthz",
    tags=["Internal"],
    summary="Local service health",
    description="Simple health check for this FastAPI service (does NOT call Pudu).",
)
async def healthz() -> dict:
    return {"status": "ok", "service": "pudu-trash-route-api"}


# ---------- Pudu Health ---------- #

@app.get(
    "/pudu/health",
    tags=["Pudu"],
    summary="Check Pudu Cloud health",
    description=(
        "Calls Pudu Cloud `/data-open-platform-service/v1/api/healthCheck`.\n\n"
        "Use this to validate:\n"
        "- `APP_KEY` / `APP_SECRET` (application credentials)\n"
        "- `PUDU_BASE_URL` (correct region + `/pudu-entry`)\n"
        "- HMAC-SHA1 signing implementation.\n"
    ),
)
async def pudu_health(
    Language: Optional[str] = Header(  # noqa: N803
        default=None,
        description=LANGUAGE_HEADER_DESC,
    ),
    client: PuduClient = Depends(get_pudu_client),
) -> JSONResponse:
    try:
        result = await client.health_check(language=Language)
        return JSONResponse(content={"ok": True, "pudu_response": result})
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e!r}")


# ---------- Robots (group & list) ---------- #

@app.get(
    "/pudu/robot/groups",
    tags=["Robots"],
    summary="List robot groups",
    description=(
        "Proxy to `GET /open-platform-service/v1/robot/group/list`.\n\n"
        "Query params:\n"
        "- device (optional): device identifier\n"
        "- shop_id (optional): shop scope\n"
    ),
)
async def list_robot_groups(
    device: Optional[str] = None,
    shop_id: Optional[str] = None,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.list_robot_groups(device=device, shop_id=shop_id, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/robot/list",
    tags=["Robots"],
    summary="List robots in a group",
    description=(
        "Proxy to `GET /open-platform-service/v1/robot/list_by_device_and_group`.\n\n"
        "Required query params:\n"
        "- device: device identifier\n"
        "- group_id: group identifier\n"
    ),
)
async def list_robots_by_device_and_group(
    device: str,
    group_id: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.list_robots_by_device_and_group(
            device=device, group_id=group_id, language=Language
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Maps & points (existing + updated + new) ---------- #

@app.get(
    "/pudu/maps/store",
    tags=["Maps"],
    summary="StoreMapList: list all map names under a shop",
    description=(
        "Proxy to `GET /data-open-platform-service/v1/api/maps`.\n\n"
        "Returns the list of map names configured under a **shop_id**.\n"
        "This is not robot-specific; it is shop-level inventory.\n"
    ),
)
async def store_map_list(
    shop_id: int,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.store_map_list(shop_id=shop_id, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/maps",
    tags=["Maps"],
    summary="AvailableMaps: list maps currently available on the robot",
    description=(
        "Proxy to `GET /map-service/v1/open/list`.\n\n"
        "Required query params:\n"
        "- sn: robot serial number\n"
    ),
)
async def list_maps(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.list_maps(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/maps/current",
    tags=["Maps"],
    summary="CurrentMap: get current robot map",
    description=(
        "Proxy to `GET /map-service/v1/open/current`.\n\n"
        "Required query params:\n"
        "- sn: robot serial number\n\n"
        "Optional query params:\n"
        "- need_element: when true, returns map elements (useful if rendering).\n"
    ),
)
async def get_current_map(
    sn: str,
    need_element: Optional[bool] = None,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.get_current_map(sn=sn, need_element=need_element, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/maps/detail-v2",
    tags=["Maps"],
    summary="GetMapDetailV2: get raw map details without unit conversion",
    description=(
        "Proxy to `GET /map-service/v1/open/map`.\n\n"
        "Required query params:\n"
        "- shop_id: shop id (string per Pudu spec)\n"
        "- map_name: map name (URL-escaped if contains # or non-ASCII)\n\n"
        "This endpoint outputs the robot map data as-is (no unit conversion)."
    ),
)
async def get_map_detail_v2(
    shop_id: str,
    map_name: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.get_map_detail_v2(shop_id=shop_id, map_name=map_name, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/maps/points",
    tags=["Maps"],
    summary="GetCurrentMapPoints: list points of interest for current map",
    description=(
        "Proxy to `GET /map-service/v1/open/point`.\n\n"
        "Required query params:\n"
        "- sn: robot serial number\n"
    ),
)
async def list_points(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.list_points(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Robot position (NEW) ---------- #

@app.get(
    "/pudu/robot/position",
    tags=["Status"],
    summary="GetPosition: get robot current position on its map",
    description=(
        "Proxy to `GET /open-platform-service/v1/robot/get_position`.\n\n"
        "Required query params:\n"
        "- sn: robot serial number\n\n"
        "Response includes:\n"
        "- map_name, floor\n"
        "- position {x,y,z}\n"
    ),
)
async def get_robot_position(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.get_robot_position(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Missions / Tasks (existing) ---------- #

@app.post(
    "/pudu/missions/task-errand",
    tags=["Missions"],
    summary="Create an errand route task",
    description=(
        "Proxy to `POST /open-platform-service/v1/task_errand`.\n\n"
        "Dispatches one or more errands to the robot. Each errand contains "
        "an ordered list of map points defining a route."
    ),
)
async def create_task_errand(
    body: TaskErrandBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.create_task_errand(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/pudu/missions/transport-task",
    tags=["Missions"],
    summary="Create a transport/delivery task (legacy transport_task)",
    description=(
        "Proxy to `POST /open-platform-service/v1/transport_task`.\n\n"
        "Creates an advanced transport/delivery task "
        "(start point, multiple destinations, priority, etc.).\n\n"
        "Note: Pudu also provides `delivery_task`/`delivery_action` APIs for delivery workflows."
    ),
)
async def create_transport_task(
    body: TransportTaskBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.create_transport_task(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/pudu/custom-call",
    tags=["Missions"],
    summary="Call robot to a single point",
    description=(
        "Proxy to `POST /open-platform-service/v1/custom_call`.\n\n"
        "Calls the robot to a specific map point and optionally shows custom content "
        "on the tablet (image, QR, video, confirmation, etc.).\n\n"
        "Body fields:\n"
        "- sn (optional in wrapper model, may be required by your tenant)\n"
        "- map_name (required)\n"
        "- point (required)\n"
        "- call_device_name (required)\n"
        "- extra (optional): any additional Pudu fields (call_mode, mode_data, shop_id, etc.)"
    ),
)
async def custom_call(
    body: CustomCallBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    full_body: Dict[str, Any] = {
        "sn": body.sn,
        "map_name": body.map_name,
        "point": body.point,
        "call_device_name": body.call_device_name,
        **body.extra,
    }
    try:
        return await client.custom_call(full_body, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/pudu/custom-call/cancel",
    tags=["Missions"],
    summary="Cancel a custom call",
    description="Proxy to `POST /open-platform-service/v1/custom_call/cancel`.",
)
async def custom_call_cancel(
    body: CustomCallCancelBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.custom_call_cancel(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/pudu/custom-call/complete",
    tags=["Missions"],
    summary="Mark a custom call as complete",
    description=(
        "Proxy to `POST /open-platform-service/v1/custom_call/complete`.\n\n"
        "Marks a call as completed and optionally defines a `next_call_task` "
        "to chain another call."
    ),
)
async def custom_call_complete(
    body: CustomCallCompleteBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.custom_call_complete(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Call list (NEW) ---------- #

@app.get(
    "/pudu/calls",
    tags=["Missions"],
    summary="CallList: list call tasks for a robot",
    description=(
        "Proxy to `GET /open-platform-service/v1/call/list`.\n\n"
        "Required query params:\n"
        "- sn: robot serial number\n\n"
        "Optional query params:\n"
        "- limit: max items to return (example uses 10)\n\n"
        "Response includes `total` and `list` of call tasks "
        "(task_id, shop_id, map_name, point, queue, create_time, finish_time, status, remark, product_code...)."
    ),
)
async def list_calls(
    sn: str,
    limit: Optional[int] = Field(default=10, ge=1, le=200, description="Max items to return."),
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.list_calls(sn=sn, limit=limit, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Delivery (NEW) ---------- #

@app.post(
    "/pudu/delivery/task",
    tags=["Delivery"],
    summary="DeliveryTask: send a delivery task (multi-tray)",
    description=(
        "Proxy to `POST /open-platform-service/v1/delivery_task`.\n\n"
        "Sends a delivery task to the robot, supporting multi-tray and delivery modes.\n"
        "After initiating the task, you can subscribe to callback notifications for delivery status "
        "(notifyDeliveryTask).\n\n"
        "Key fields:\n"
        "- payload.type: NEW | MODIFY\n"
        "- payload.delivery_sort: AUTO | FIXED\n"
        "- payload.execute_task: true | false\n"
        "- payload.trays[].destinations[].destination: point name\n"
        "- payload.trays[].destinations[].id: order id (may be optional per docs)\n"
        "- payload.trays[].destinations[].phone_num / phone_code optional\n"
        "- payload.trays[].destinations[].map_info.map_name optional\n"
    ),
)
async def delivery_task(
    body: DeliveryTaskBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.delivery_task(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/pudu/delivery/action",
    tags=["Delivery"],
    summary="DeliveryAction: operate delivery tasks (start/complete/cancel/pause/resume)",
    description=(
        "Proxy to `POST /open-platform-service/v1/delivery_action`.\n\n"
        "Prerequisite: ensure the robot is executing a delivery task.\n\n"
        "Actions:\n"
        "- START\n"
        "- COMPLETE\n"
        "- CANCEL_ALL_DELIVERY\n"
        "- PAUSE (T300 only)\n"
        "- RESUME (T300 only)\n"
    ),
)
async def delivery_action(
    body: DeliveryActionBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.delivery_action(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Status & Position (existing) ---------- #

@app.get(
    "/pudu/status/by-sn",
    tags=["Status"],
    summary="Get status for one robot",
    description="Proxy to `GET /open-platform-service/v2/status/get_by_sn`.",
)
async def get_status_by_sn(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.get_status_by_sn(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/status/by-group",
    tags=["Status"],
    summary="Get status and position for a group of robots",
    description="Proxy to `GET /open-platform-service/v1/status/get_by_group_id`.",
)
async def get_status_by_group_id(
    group_id: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.get_status_by_group_id(group_id=group_id, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post(
    "/pudu/status/position-command",
    tags=["Status"],
    summary="Start periodic position reporting (NotifyRobotReportPosition)",
    description=(
        "Proxy to `POST /open-platform-service/v1/position_command`.\n\n"
        "Requests a robot to publish its position every `interval` seconds "
        "for `times` occurrences.\n"
        "Les positions seront reçues sur l'endpoint de callback `notifyRobotPose`."
    ),
)
async def position_command(
    body: PositionCommandBody,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.position_command(body.dict(), language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/tasks/state",
    tags=["Status"],
    summary="Get robot task state",
    description="Proxy to `GET /open-platform-service/v1/robot/task/state/get`.",
)
async def get_robot_task_state(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.get_robot_task_state(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Recharge ---------- #

@app.get(
    "/pudu/recharge/v1",
    tags=["Utility"],
    summary="Send robot to recharge (v1)",
    description="Proxy to `GET /open-platform-service/v1/recharge`.",
)
async def recharge_v1(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.recharge_v1(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get(
    "/pudu/recharge/v2",
    tags=["Utility"],
    summary="Send robot to recharge (v2, MQTT)",
    description="Proxy to `GET /open-platform-service/v2/recharge`.",
)
async def recharge_v2(
    sn: str,
    Language: Optional[str] = Header(default=None, description=LANGUAGE_HEADER_DESC),  # noqa: N803
    client: PuduClient = Depends(get_pudu_client),
):
    try:
        return await client.recharge_v2(sn=sn, language=Language)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------- Callback endpoint – notifyRobotPose ---------- #

@app.post(
    "/pudu/callback/robotPose",
    tags=["Callbacks"],
    summary="Callback Pudu – notifyRobotPose (position du robot)",
    description=(
        "Endpoint appelé par Pudu Cloud pour le callback "
        "`RobotPose-notifyRobotPose`.\n\n"
        "Configure cette URL comme `Callback URL` dans le portail Pudu (au niveau de l'APPKey)."
    ),
)
async def robot_pose_callback(body: RobotPoseCallback, request: Request):
    if body.callback_type != "notifyRobotPose":
        raise HTTPException(status_code=400, detail="Unsupported callback_type")

    pose = body.data

    logger.info(
        "PUDU notifyRobotPose from %s: sn=%s x=%.3f y=%.3f yaw=%.3f ts=%d notify_ts=%d",
        request.client.host if request.client else "unknown",
        pose.sn,
        pose.x,
        pose.y,
        pose.yaw,
        pose.timestamp,
        pose.notify_timestamp,
    )

    return {"status": "ok", "sn": pose.sn}
