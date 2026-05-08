
"""Error envelope logging tasks for the dummy streaming smoke."""

from __future__ import annotations

from typing import Any

from loom.streaming import ErrorEnvelope, Message, RecordStep
from loom.streaming.kafka import DecodeError


def _payload_value(payload: object, key: str) -> object | None:
    if isinstance(payload, dict):
        return payload.get(key)
    return getattr(payload, key, None)


class PrintErrorEnvelopeTask(RecordStep[ErrorEnvelope[Any] | DecodeError, ErrorEnvelope[Any]]):
    """Log an error envelope and pass it through unchanged."""

    def execute(
        self,
        message: Message[ErrorEnvelope[Any] | DecodeError],
        **_kwargs: object,
    ) -> ErrorEnvelope[Any]:
        payload = message.payload
        if isinstance(payload, DecodeError):
            envelope = payload.error
            self.log.debug(
                "error_envelope",
                kind=envelope.kind,
                reason=envelope.reason,
                payload_type=payload.loom_message_type(),
                original_type=None,
                original_kind=None,
                original_request_id=None,
            )
            return envelope

        envelope = payload
        original = envelope.original_message
        original_message_type = None
        original_kind = None
        original_request_id = None
        if original is not None:
            original_message_type = original.meta.message_type
            original_kind = _payload_value(original.payload, "kind")
            original_request_id = _payload_value(original.payload, "request_id")
        self.log.debug(
            "error_envelope",
            kind=envelope.kind,
            reason=envelope.reason,
            payload_type=envelope.payload_type,
            original_type=original_message_type,
            original_kind=original_kind,
            original_request_id=original_request_id,
        )
        return envelope


class PrintBusinessErrorTask(RecordStep[ErrorEnvelope[Any], ErrorEnvelope[Any]]):
    """Log business errors with a distinct marker and pass them through."""

    def execute(
        self,
        message: Message[ErrorEnvelope[Any] | DecodeError],
        **_kwargs: object,
    ) -> ErrorEnvelope[Any]:
        envelope = message.payload
        original = envelope.original_message
        original_message_type = None
        original_request_id = None
        if original is not None:
            original_message_type = original.meta.message_type
            original_request_id = _payload_value(original.payload, "request_id")
        self.log.debug(
            "business_error_envelope",
            kind=envelope.kind,
            reason=envelope.reason,
            payload_type=envelope.payload_type,
            original_type=original_message_type,
            original_request_id=original_request_id,
        )
        return envelope
