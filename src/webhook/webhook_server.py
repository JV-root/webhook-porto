from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# ======================================================================
# API
# ======================================================================
app = FastAPI(
    title="Tech4 Webhook Receiver",
    version="1.0.0",
    description=(
        "Webhook para receber callbacks da Tech4.ai (Conversation Management) e "
        "expor endpoints simples para leitura do último evento recebido."
    ),
)

# ======================================================================
# In-memory store
# Key: serviceId (identificador real da conversa)
# Value: último evento recebido
# ======================================================================
WEBHOOK_STORE: Dict[str, Dict[str, Any]] = {}

# ======================================================================
# Idempotency store
# Guarda IDs de eventos já processados
# (trocar por Redis em produção)
# ======================================================================
PROCESSED_EVENT_IDS: Set[str] = set()

# ======================================================================
# Models – CloudEvents (Recebimento de Mensagem)
# ======================================================================
class MessageData(BaseModel):
    id: str
    type: str              # "text"
    createdAt: str
    sentAt: str
    by: str                # system | user | automation
    serviceId: str
    text: Optional[str] = None


class CloudEventMessage(BaseModel):
    specversion: str
    id: str                # event id
    type: str              # amber.service:conversation:message
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


def get_latest_event_or_404(service_id: str) -> Dict[str, Any]:
    data = WEBHOOK_STORE.get(service_id)
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No messages found for this serviceId",
        )
    return data

# ======================================================================
# Home
# ======================================================================
@app.get("/", tags=["Docs"], include_in_schema=False)
async def home():
    return {
        "service": "tech4-webhook-receiver",
        "now_utc": utc_now_iso(),
        "endpoints": {
            "POST /webhooks/tech4": "Recebe mensagens (CloudEvents)",
            "GET  /sessions/{session_id}/latest": "Última mensagem da conversa (serviceId)",
            "DELETE /sessions/{session_id}": "Remove conversa do store",
            "GET  /health": "Healthcheck",
            "GET  /docs": "Swagger UI",
        },
    }

# ======================================================================
# Healthcheck
# ======================================================================
@app.get("/health", tags=["Ops"])
async def health():
    return {"status": "up", "now_utc": utc_now_iso()}

# ======================================================================
# Webhook – Recebimento de Mensagem (com idempotência)
# ======================================================================
@app.post(
    "/webhooks/tech4",
    tags=["Tech4 Webhook"],
    summary="Recebe mensagens via CloudEvents",
)
async def tech4_webhook(event: CloudEventMessage):
    # 1. Aceita apenas eventos de mensagem
    if event.type != "amber.service:conversation:message":
        return {"status": "ignored"}

    # 2. Idempotência por event.id
    if event.id in PROCESSED_EVENT_IDS:
        return {"status": "ignored", "reason": "duplicate event"}

    # Marca evento como processado
    PROCESSED_EVENT_IDS.add(event.id)

    # 3. Aceita apenas mensagens de texto
    if event.data.type != "text":
        return {"status": "ignored"}

    service_id = event.data.serviceId

    # 4. Persiste última mensagem da conversa
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

    return {"status": "ok"}

# ======================================================================
# Sessions (leitura simples por serviceId)
# ======================================================================
@app.get(
    "/sessions/{session_id}/latest",
    tags=["Sessions"],
    summary="Retorna a última mensagem da conversa (serviceId)",
)
async def get_latest_session_event(session_id: str):
    """
    session_id representa o serviceId conforme a documentação oficial.
    """
    service_id = session_id
    return get_latest_event_or_404(service_id)


@app.delete(
    "/sessions/{session_id}",
    tags=["Sessions"],
    summary="Remove a conversa (serviceId) do store",
)
async def delete_session(session_id: str):
    service_id = session_id

    if service_id in WEBHOOK_STORE:
        del WEBHOOK_STORE[service_id]
        return {"status": "deleted", "service_id": service_id}

    raise HTTPException(
        status_code=404,
        detail="serviceId not found",
    )


@app.get(
    "/sessions",
    tags=["Sessions"],
    summary="Lista serviceIds armazenados (debug)",
)
async def list_sessions(limit: int = 50):
    keys = list(WEBHOOK_STORE.keys())[: max(1, min(limit, 500))]
    return {"count": len(keys), "service_ids": keys}

# ======================================================================
# Main (dev)
# ======================================================================
if __name__ == "__main__":
    uvicorn.run(
        "webhook_server:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
    )
