from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Body
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


def k_to(to: str) -> str:
    return f"tech4:to:{to}"

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
# Webhook – usando "to" como chave de correlação
# ======================================================================
@router.post(
    "/webhooks/tech4/862001453668864/messages",
    summary="Webhook genérico (correlação por campo 'to')",
)
async def tech4_webhook_open(
    payload: Any = Body(
        ...,
        description="Payload genérico (qualquer JSON será aceito, campo 'to' usado como chave)",
        example={
            "to": "5511999999999",
            "message": {
                "text": "Olá!"
            }
        },
    ),
):
    body: Dict[str, Any] = payload if isinstance(payload, dict) else {"payload": payload}

    # --------------------------------------------------------------
    # Extrair campo "to" (nova chave de correlação)
    # --------------------------------------------------------------
    to = body.get("to") or "unknown"

    # --------------------------------------------------------------
    # Persistir payload exatamente como recebido
    # --------------------------------------------------------------
    redis.setex(
        k_to(to),
        REDIS_TTL_SECONDS,
        json.dumps(body),
    )

    return {
        "status": "stored",
        "to": to,
        "ttl_seconds": REDIS_TTL_SECONDS,
    }

# ======================================================================
# Latest – agora baseado em "to"
# ======================================================================
@router.get(
    "/messages/{to}/latest",
    summary="Retorna o último payload recebido para o destino 'to'",
)
async def get_latest_by_to(to: str):
    data = redis.get(k_to(to))
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No payload found for this 'to'",
        )
    return json.loads(data)


@router.delete(
    "/messages/{to}",
    summary="Remove histórico do destino 'to'",
)
async def delete_by_to(to: str):
    deleted = redis.delete(k_to(to))
    if deleted:
        return {
            "status": "deleted",
            "backend": "redis",
            "to": to,
        }
    raise HTTPException(
        status_code=404,
        detail="'to' not found",
    )
