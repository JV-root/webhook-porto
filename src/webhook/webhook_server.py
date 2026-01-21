from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

WEBHOOK_STORE: Dict[str, Dict[str, Any]] = {}
PROCESSED_EVENT_IDS: Set[str] = set()

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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_latest_event_or_404(service_id: str) -> Dict[str, Any]:
    data = WEBHOOK_STORE.get(service_id)
    if not data:
        raise HTTPException(status_code=404, detail="No messages found for this serviceId")
    return data


@router.get("/", tags=["Docs"], include_in_schema=False)
async def home():
    return {
        "service": "tech4-webhook-receiver",
        "now_utc": utc_now_iso(),
        "endpoints": {
            "POST /webhooks/tech4": "Recebe mensagens (CloudEvents) - LEGACY",
            "GET  /sessions/{session_id}/latest": "Última mensagem (LEGACY)",
            "DELETE /sessions/{session_id}": "Remove conversa (LEGACY)",
            "GET  /health": "Healthcheck",
            "GET  /docs": "Swagger UI",
        },
    }


@router.get("/health", tags=["Ops"])
async def health():
    return {"status": "up", "now_utc": utc_now_iso()}


@router.post("/webhooks/tech4", tags=["in-memory"])
async def tech4_webhook(event: CloudEventMessage):
    if event.type != "amber.service:conversation:message":
        return {"status": "ignored"}

    if event.id in PROCESSED_EVENT_IDS:
        return {"status": "ignored", "reason": "duplicate event"}

    PROCESSED_EVENT_IDS.add(event.id)

    if event.data.type != "text":
        return {"status": "ignored"}

    service_id = event.data.serviceId

    WEBHOOK_STORE[service_id] = {
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

    return {"status": "ok", "backend": "legacy"}


@router.get("/sessions/{session_id}/latest", tags=["in-memory"])
async def get_latest_session_event(session_id: str):
    return get_latest_event_or_404(session_id)


@router.delete("/sessions/{session_id}", tags=["in-memory"])
async def delete_session(session_id: str):
    if session_id in WEBHOOK_STORE:
        del WEBHOOK_STORE[session_id]
        return {"status": "deleted", "service_id": session_id}
    raise HTTPException(status_code=404, detail="serviceId not found")


@router.get("/sessions", tags=["in-memory"])
async def list_sessions(limit: int = 50):
    keys = list(WEBHOOK_STORE.keys())[: max(1, min(limit, 500))]
    return {"count": len(keys), "service_ids": keys}