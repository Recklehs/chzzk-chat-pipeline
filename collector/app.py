import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from email.parser import BytesParser
from email.policy import default as email_policy
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs

from fastapi import Body, Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyHeader
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

from collector.config import load_settings_from_env
from collector.control import (
    AppSettings,
    ChannelCommandError,
    ChannelStore,
    CmdCounter,
    LiveStatusClient,
    MonitorCoordinator,
    STOP_REASON_CHANNEL_REMOVED,
    STOP_REASON_MANUAL_STOP,
    apply_control_command,
    normalize_channel_input,
)
from collector.metrics import CONTENT_TYPE_LATEST, metrics
from collector.publisher import build_default_raw_publisher
from collector.redis.cache import build_json_cache
from collector.redis.keys import DASHBOARD_REALTIME_TTL_SECONDS, dashboard_realtime_key
from collector.runtime import connect_to_chzzk, print_stats_periodically

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
logger = logging.getLogger(__name__)


class EnabledPayload(BaseModel):
    enabled: bool


async def get_control_auth(request: Request, api_key: str = Security(api_key_header)):
    expected_api_key = request.app.state.settings.api_key
    if api_key == expected_api_key:
        return "api-key"
    if request.session.get("authenticated"):
        return "session"
    raise HTTPException(status_code=403, detail="Could not validate credentials")


async def require_dashboard_session(request: Request):
    if request.session.get("authenticated"):
        return True
    raise HTTPException(status_code=401, detail="Dashboard login required.")


def serialize_channel_state(app: FastAPI, channel_payload: dict) -> dict:
    return channel_payload


def resolve_channel_by_alias(store: ChannelStore, alias: str):
    return next((channel for channel in store.list_channels() if channel.alias == alias), None)


async def extract_dashboard_api_key(request: Request) -> str:
    raw_content_type = request.headers.get("content-type", "").strip()
    content_type = raw_content_type.split(";", 1)[0].strip().lower()

    if content_type == "application/json":
        payload = await request.json()
        if isinstance(payload, dict):
            return str(payload.get("api_key", "")).strip()
        return ""

    if content_type == "application/x-www-form-urlencoded":
        raw_body = await request.body()
        parsed = parse_qs(raw_body.decode("utf-8", errors="replace"))
        return parsed.get("api_key", [""])[0].strip()

    if content_type == "multipart/form-data":
        raw_body = await request.body()
        message = BytesParser(policy=email_policy).parsebytes(
            b"Content-Type: " + raw_content_type.encode("utf-8") + b"\r\n\r\n" + raw_body
        )
        if not message.is_multipart():
            return ""

        for part in message.iter_parts():
            if part.get_param("name", header="content-disposition") != "api_key":
                continue
            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace").strip()
        return ""

    return request.query_params.get("api_key", "").strip()


def build_app(
    *,
    resolved_settings: AppSettings,
    resolved_live_status_client: LiveStatusClient,
    resolved_raw_publisher,
    counter: CmdCounter,
    store: ChannelStore,
    coordinator: MonitorCoordinator,
    start_background_tasks: bool = True,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app_instance: FastAPI):
        cleanup_errors = []
        try:
            await resolved_raw_publisher.start()
            app_instance.state.raw_publisher_started = True
            await coordinator.startup(start_background_tasks=start_background_tasks)
            app_instance.state.coordinator_started = True
            if start_background_tasks:
                app_instance.state.stats_task = asyncio.create_task(print_stats_periodically(counter, 30))
            yield
        finally:
            if app_instance.state.stats_task is not None and not app_instance.state.stats_task.done():
                app_instance.state.stats_task.cancel()
                await asyncio.gather(app_instance.state.stats_task, return_exceptions=True)
            if app_instance.state.coordinator_started:
                try:
                    await coordinator.shutdown()
                except Exception as exc:
                    cleanup_errors.append(exc)
            if app_instance.state.raw_publisher_started:
                try:
                    await resolved_raw_publisher.stop()
                except Exception as exc:
                    cleanup_errors.append(exc)
            redis_cache = getattr(app_instance.state, "redis_cache", None)
            if redis_cache is not None and hasattr(redis_cache, "close"):
                try:
                    await redis_cache.close()
                except Exception:
                    logger.warning("redis cache close failed", exc_info=True)
            if cleanup_errors and sys.exc_info()[1] is None:
                raise cleanup_errors[0]

    app = FastAPI(title="Chzzk Collector Control API", lifespan=lifespan)
    app.add_middleware(SessionMiddleware, secret_key=resolved_settings.session_secret_key, same_site="lax")

    app.state.settings = resolved_settings
    app.state.counter = counter
    app.state.store = store
    app.state.live_status_client = resolved_live_status_client
    app.state.raw_publisher = resolved_raw_publisher
    app.state.coordinator = coordinator
    app.state.redis_cache = build_json_cache(resolved_settings)
    app.state.start_background_tasks = start_background_tasks
    app.state.stats_task = None
    app.state.raw_publisher_started = False
    app.state.coordinator_started = False

    if resolved_settings.metrics_enabled:
        @app.get("/metrics")
        async def get_metrics():
            return Response(content=metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/channels", status_code=202, dependencies=[Depends(get_control_auth)])
    async def add_channel(
        request: Request,
        channel_id: Optional[str] = None,
        streamer_nickname: Optional[str] = None,
        channel_input: Optional[str] = None,
        alias: Optional[str] = None,
        enabled: bool = True,
    ):
        requested_input = channel_input or channel_id
        try:
            parsed_channel_id = normalize_channel_input(requested_input or "")
            await apply_control_command(
                store=request.app.state.store,
                live_status_client=request.app.state.live_status_client,
                coordinator=request.app.state.coordinator,
                action="add",
                channels=[
                    {
                        "channel_input": requested_input,
                        "alias": alias if alias is not None else streamer_nickname,
                    }
                ],
            )
            if not enabled:
                request.app.state.store.set_enabled(parsed_channel_id, False)
                await request.app.state.coordinator.stop_monitoring(parsed_channel_id, STOP_REASON_MANUAL_STOP)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ChannelCommandError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

        state = await request.app.state.coordinator.dashboard_state()
        channel_payload = next(channel for channel in state["channels"] if channel["channel_id"] == parsed_channel_id)

        return {
            "message": "Channel monitoring updated",
            "channel_id": parsed_channel_id,
            "channel": serialize_channel_state(request.app, channel_payload),
        }

    @app.patch("/channels/{channel_id}/enabled", dependencies=[Depends(get_control_auth)])
    async def update_channel_enabled(
        request: Request,
        channel_id: str,
        payload: EnabledPayload | None = Body(default=None),
        enabled: Optional[bool] = None,
    ):
        resolved_enabled = payload.enabled if payload is not None else enabled
        if resolved_enabled is None:
            raise HTTPException(status_code=400, detail="'enabled' is required.")

        existing = request.app.state.store.get_channel(channel_id)
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Channel with id '{channel_id}' not found.")

        request.app.state.store.set_enabled(channel_id, resolved_enabled)
        if resolved_enabled:
            snapshot = await request.app.state.live_status_client.fetch(channel_id)
            request.app.state.store.update_snapshot(channel_id, snapshot)
            await request.app.state.coordinator.reconcile_single_channel(channel_id, snapshot)
        else:
            await request.app.state.coordinator.stop_monitoring(channel_id, STOP_REASON_MANUAL_STOP)

        state = await request.app.state.coordinator.dashboard_state()
        channel_payload = next(channel for channel in state["channels"] if channel["channel_id"] == channel_id)
        return {
            "message": "Channel enabled state updated",
            "channel_id": channel_id,
            "enabled": resolved_enabled,
            "channel": channel_payload,
        }

    @app.delete("/channels", status_code=200, dependencies=[Depends(get_control_auth)])
    async def delete_channel(
        request: Request,
        channel_id: Optional[str] = None,
        streamer_nickname: Optional[str] = None,
    ):
        if channel_id is None and streamer_nickname is None:
            raise HTTPException(
                status_code=400,
                detail="Query parameter 'channel_id' or 'streamer_nickname' is required.",
            )
        if channel_id is not None and streamer_nickname is not None:
            raise HTTPException(
                status_code=400,
                detail="Please provide *either* 'channel_id' *or* 'streamer_nickname', not both.",
            )

        target_channel_id = channel_id
        if target_channel_id is None:
            channel = resolve_channel_by_alias(request.app.state.store, streamer_nickname)
            if channel is None:
                raise HTTPException(status_code=404, detail=f"Channel with nickname '{streamer_nickname}' not found.")
            target_channel_id = channel.channel_id

        existing = request.app.state.store.get_channel(target_channel_id)
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Channel with id '{target_channel_id}' not found.")

        await request.app.state.coordinator.stop_monitoring(target_channel_id, STOP_REASON_CHANNEL_REMOVED)
        request.app.state.store.delete_channel(target_channel_id)

        return {
            "message": "Channel deleted",
            "channel_id": target_channel_id,
            "streamer_nickname": existing.alias,
        }

    @app.get("/channels", dependencies=[Depends(get_control_auth)])
    async def list_channels(request: Request):
        state = await request.app.state.coordinator.dashboard_state()
        return {
            "monitoring_count": state["summary"]["monitoring_channel_count"],
            "channels": state["channels"],
        }

    @app.get("/stats", dependencies=[Depends(get_control_auth)])
    async def get_stats(request: Request):
        state = await request.app.state.coordinator.dashboard_state()
        return {
            "collected_cmds": state["collected_cmds"],
            "total_collected_count": state["summary"]["total_collected_count"],
            "recent_events_per_minute": state["summary"]["recent_events_per_minute"],
            "monitoring_count": state["summary"]["monitoring_channel_count"],
        }

    @app.get("/dashboard")
    async def dashboard(request: Request):
        if not request.session.get("authenticated"):
            return TEMPLATES.TemplateResponse(
                request,
                "dashboard_login.html",
                {
                    "title": "CHZZK Monitoring Dashboard Login",
                },
            )
        return TEMPLATES.TemplateResponse(
            request,
            "dashboard.html",
            {
                "title": "CHZZK Monitoring Dashboard",
                "refresh_seconds": request.app.state.settings.dashboard_refresh_seconds,
            },
        )

    @app.post("/dashboard/session")
    async def create_dashboard_session(request: Request):
        api_key = await extract_dashboard_api_key(request)
        if api_key != request.app.state.settings.api_key:
            raise HTTPException(status_code=401, detail="Invalid API key.")
        request.session["authenticated"] = True
        return JSONResponse({"authenticated": True})

    @app.delete("/dashboard/session")
    async def delete_dashboard_session(request: Request, _auth=Depends(require_dashboard_session)):
        request.session.clear()
        return JSONResponse({"authenticated": False})

    @app.get("/dashboard/api/state", dependencies=[Depends(require_dashboard_session)])
    async def dashboard_state(request: Request, window: str = "current"):
        async def load_state():
            state = await request.app.state.coordinator.dashboard_state()
            state["refresh_seconds"] = request.app.state.settings.dashboard_refresh_seconds
            return state

        return await request.app.state.redis_cache.get_or_set_json_cache(
            dashboard_realtime_key(window=window),
            DASHBOARD_REALTIME_TTL_SECONDS,
            load_state,
        )

    return app


def create_app(
    settings: AppSettings | None = None,
    live_status_client: LiveStatusClient | None = None,
    raw_publisher=None,
    monitor_task_factory=None,
    start_background_tasks: bool = True,
) -> FastAPI:
    resolved_settings = settings or load_settings_from_env()
    resolved_live_status_client = live_status_client or LiveStatusClient(
        base_url=resolved_settings.chzzk_api_base_url,
        timeout_seconds=resolved_settings.chzzk_api_timeout_seconds,
    )
    resolved_raw_publisher = raw_publisher or build_default_raw_publisher(resolved_settings)
    resolved_monitor_task_factory = monitor_task_factory or connect_to_chzzk

    counter = CmdCounter()
    store = ChannelStore(resolved_settings.control_db_path)
    coordinator = MonitorCoordinator(
        store=store,
        live_status_client=resolved_live_status_client,
        raw_publisher=resolved_raw_publisher,
        counter=counter,
        poll_interval_seconds=resolved_settings.chzzk_live_poll_seconds,
        poll_max_interval_seconds=resolved_settings.chzzk_live_poll_max_seconds,
        monitor_task_factory=resolved_monitor_task_factory,
    )

    return build_app(
        resolved_settings=resolved_settings,
        resolved_live_status_client=resolved_live_status_client,
        resolved_raw_publisher=resolved_raw_publisher,
        counter=counter,
        store=store,
        coordinator=coordinator,
        start_background_tasks=start_background_tasks,
    )
