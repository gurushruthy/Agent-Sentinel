from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from agent_sentinel.api.routes.cluster import router as cluster_router
from agent_sentinel.api.routes.tasks import router as tasks_router

STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_app() -> FastAPI:
    app = FastAPI(
        title="Agent-Sentinel API",
        version="0.1.0",
        description="REST API facade for Agent-Sentinel control plane",
    )

    @app.get("/healthz", tags=["system"])
    def healthz():
        return {"ok": True}

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", tags=["system"])
    def root():
        return FileResponse(STATIC_DIR / "dashboard.html")

    @app.get("/ui", tags=["system"])
    def ui():
        return FileResponse(STATIC_DIR / "dashboard.html")

    app.include_router(tasks_router)
    app.include_router(cluster_router)
    return app


app = create_app()
