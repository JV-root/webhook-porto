import os
from fastapi import FastAPI
import uvicorn

from src.webhook.webhook_server import router as legacy_router
from src.webhook.redis_webhook_server import router as redis_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Tech4 Webhook Receiver",
        version="2.0.0",
        description="App principal agregando legacy (memória) + redis (persistência)",
    )

    # inclui legacy (paths /webhooks, /sessions, /health)
    app.include_router(legacy_router)

    # inclui redis (prefix /redis já vem no router)
    app.include_router(redis_router)

    return app


if __name__ == "__main__":
    uvicorn.run(
        "main:create_app",
        factory=True,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        reload=True,
    )