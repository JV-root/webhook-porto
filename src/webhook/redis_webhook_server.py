from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from redis import Redis

# ======================================================================
# Config Redis
# ======================================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://stratosphere.yaman.com.br:6379/0")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "86400"))  # 1 dia

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# ======================================================================
# Router (somente endpoints Redis)
# ======================================================================
router = APIRouter(prefix="/redis", tags=["Redis"])

# ======================================================================
# Models – CloudEvents (mesmo payload do legado)
# ======================================================================
class MessageData(BaseModel):
    id: str
    type: str
    createdAt: str
    sentAt: str
    by: str
    serviceId: str
    text: Optional[str] = None


class CloudEventMessage(BaseModel):
    specversion: str
    id: str
    type: str
    source: str
    subject: str
    time: str
    datacontenttype: str
    data: MessageData


# ======================================================================
# Helpers
# ======================================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def k_event(event_id: str) -> str:
    return f"tech4:event:{event_id}"


def k_session(service_id: str) -> str:
    return f"tech4:session:{service_id}"


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
# Webhook – Redis
# ======================================================================
@router.post("/webhooks/tech4", summary="Recebe mensagens e persiste no Redis (TTL + idempotência)")
async def tech4_webhook_redis(event: CloudEventMessage):
    if event.type != "amber.service:conversation:message":
        return {"status": "ignored", "reason": "unsupported type"}

    # Idempotência via Redis
    if redis.exists(k_event(event.id)):
        return {"status": "ignored", "reason": "duplicate event"}

    redis.setex(k_event(event.id), REDIS_TTL_SECONDS, "1")

    if event.data.type != "text":
        return {"status": "ignored", "reason": "unsupported data.type"}

    service_id = event.data.serviceId

    payload = {
        "service_id": service_id,
        "event_id": event.id,
        "message_id": event.data.id,
        "text": event.data.text or "",
        "by": event.data.by,
        "created_at": event.data.createdAt,
        "sent_at": event.data.sentAt,
        "received_at": utc_now_iso(),
        "raw": event.model_dump(),
    }

    redis.setex(k_session(service_id), REDIS_TTL_SECONDS, json.dumps(payload))

    return {"status": "stored", "backend": "redis", "service_id": service_id, "ttl_seconds": REDIS_TTL_SECONDS}


# ======================================================================
# Sessions – Redis
# ======================================================================
@router.get("/sessions/{service_id}/latest", summary="Última mensagem da conversa (Redis)")
async def get_latest_session_redis(service_id: str):
    data = redis.get(k_session(service_id))
    if not data:
        raise HTTPException(status_code=404, detail="No messages found for this serviceId (Redis)")
    return json.loads(data)


@router.delete("/sessions/{service_id}", summary="Remove conversa do Redis")
async def delete_session_redis(service_id: str):
    deleted = redis.delete(k_session(service_id))
    if deleted:
        return {"status": "deleted", "backend": "redis", "service_id": service_id}
    raise HTTPException(status_code=404, detail="serviceId not found (Redis)")