import uvicorn

from collector.app import build_app
from collector.config import load_settings_from_env
from collector.control import *  # noqa: F401,F403
from collector.publisher import *  # noqa: F401,F403
from collector.runtime import *  # noqa: F401,F403


def create_app(
    settings: AppSettings | None = None,
    live_status_client: LiveStatusClient | None = None,
    raw_publisher=None,
    monitor_task_factory=None,
    start_background_tasks: bool = True,
):
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


app = create_app()


if __name__ == "__main__":
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        print("\n[INFO] 강제 종료 (KeyboardInterrupt).")
