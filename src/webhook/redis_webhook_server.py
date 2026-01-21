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
# Webhook – ingestão aberta (correlação por "to")
# ======================================================================
@router.post(
    "/webhooks/tech4/862001453668864/messages",
    summary="Webhook genérico (aceita qualquer payload, correlação por campo 'to')",
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
    # Garante que sempre trabalhamos com dict
    body: Dict[str, Any] = payload if isinstance(payload, dict) else {"payload": payload}

    # --------------------------------------------------------------
    # Nova chave de correlação: campo "to"
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
# Latest – leitura alinhada ao campo "to"
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

    # Retorna exatamente o payload salvo
    return json.loads(data)

# ======================================================================
# Delete – limpeza por "to"
# ======================================================================
@router.delete(
    "/messages/{to}",
    summary="Remove payload associado ao destino 'to'",
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
