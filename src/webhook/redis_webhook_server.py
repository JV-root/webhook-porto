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
MAX_MESSAGES_PER_TO = int(os.getenv("MAX_MESSAGES_PER_TO", "1000"))

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


def k_to_messages(to: str) -> str:
    return f"tech4:to:{to}:messages"

# ======================================================================
# Ops
# ======================================================================
@router.get("/health", summary="Healthcheck Redis")
async def redis_health():
    return {
        "status": "up",
        "now_utc": utc_now_iso(),
        "ttl_seconds": REDIS_TTL_SECONDS,
        "max_messages_per_to": MAX_MESSAGES_PER_TO,
        "redis_url": REDIS_URL,
    }

# ======================================================================
# Webhook – ingestão aberta (histórico por "to")
# ======================================================================
@router.post(
    "/webhooks/tech4/862001453668864/messages",
    summary="Webhook genérico (aceita qualquer payload, histórico por campo 'to')",
)
async def tech4_webhook_open(
    payload: Any = Body(
        ...,
        description="Payload genérico (qualquer JSON será aceito; campo 'to' usado como chave)",
        example={
            "to": "5511999999999",
            "message": {
                "text": "Olá!"
            }
        },
    ),
):
    body: Dict[str, Any] = payload if isinstance(payload, dict) else {"payload": payload}

    to = body.get("to") or "unknown"

    stored_payload = {
        "received_at": utc_now_iso(),
        "payload": body,
    }

    key = k_to_messages(to)

    # Adiciona ao histórico
    redis.rpush(key, json.dumps(stored_payload))

    # TTL e limite
    redis.expire(key, REDIS_TTL_SECONDS)
    redis.ltrim(key, -MAX_MESSAGES_PER_TO, -1)

    return {
        "status": "stored",
        "to": to,
        "ttl_seconds": REDIS_TTL_SECONDS,
    }

# ======================================================================
# Latest – retorna APENAS o último payload
# ======================================================================
@router.get(
    "/messages/{to}/latest",
    summary="Retorna o último payload recebido para o destino 'to'",
)
async def get_latest_by_to(to: str):
    key = k_to_messages(to)

    message = redis.lindex(key, -1)
    if not message:
        raise HTTPException(
            status_code=404,
            detail="No payload found for this 'to'",
        )

    return json.loads(message)

# ======================================================================
# Delete – limpa histórico do "to"
# ======================================================================
@router.delete(
    "/messages/{to}",
    summary="Remove histórico associado ao destino 'to'",
)
async def delete_by_to(to: str):
    deleted = redis.delete(k_to_messages(to))
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
