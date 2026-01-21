from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request, Body
from redis import Redis

# ======================================================================
# Config Redis
# ======================================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "86400"))  # 1 dia

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# ======================================================================
# Router
# ======================================================================
router = APIRouter(prefix="/redis", tags=["Redis"])

# ======================================================================
# Helpers
# ======================================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def k_session(session_id: str) -> str:
    return f"tech4:session:{session_id}"

# ======================================================================
# Ops
# ======================================================================
@router.get("/health", summary="Healthcheck Redis")
async def redis_health():
    return {
        "status": "up",
        "now_utc": utc_now_iso(),
        "ttl_seconds": REDIS_TTL_SECONDS,
        "redis_url": REDIS_URL,
    }

# ======================================================================
# Webhook – TOTALMENTE ABERTO (Swagger com body genérico)
# ======================================================================
@router.post(
    "/webhooks/tech4",
    summary="Webhook genérico (aceita qualquer payload e persiste bruto)",
)
async def tech4_webhook_open(
    payload: Any = Body(
        ...,
        description="Payload genérico (qualquer JSON será aceito)",
        example={
            "qualquer": "estrutura",
            "pode": {
                "ser": ["o", "que", "você", "quiser"]
            }
        },
    ),
):
    body: Dict[str, Any] = payload if isinstance(payload, dict) else {"payload": payload}

    # --------------------------------------------------------------
    # Definir session_id de forma flexível
    # --------------------------------------------------------------
    session_id = (
        ((body.get("data") or {}).get("service") or {}).get("id")
        or body.get("session_id")
        or body.get("id")
        or "default"
    )

    # --------------------------------------------------------------
    # Persistir payload exatamente como recebido
    # --------------------------------------------------------------
    data_to_store = {
        "session_id": session_id,
        "received_at": utc_now_iso(),
        "payload": body,
    }

    redis.setex(
        k_session(session_id),
        REDIS_TTL_SECONDS,
        json.dumps(data_to_store),
    )

    return {
        "status": "stored",
        "session_id": session_id,
        "ttl_seconds": REDIS_TTL_SECONDS,
    }

# ======================================================================
# Sessions – Redis
# ======================================================================
@router.get(
    "/sessions/{session_id}/latest",
    summary="Retorna o último payload recebido (bruto)",
)
async def get_latest_session_redis(session_id: str):
    data = redis.get(k_session(session_id))
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No payload found for this session_id",
        )
    return json.loads(data)


@router.delete(
    "/sessions/{session_id}",
    summary="Remove sessão do Redis",
)
async def delete_session_redis(session_id: str):
    deleted = redis.delete(k_session(session_id))
    if deleted:
        return {
            "status": "deleted",
            "backend": "redis",
            "session_id": session_id,
        }
    raise HTTPException(
        status_code=404,
        detail="session_id not found",
    )
