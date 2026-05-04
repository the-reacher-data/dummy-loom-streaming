"""Error envelope logging tasks for the dummy streaming smoke."""

from __future__ import annotations

import logging
from typing import Any

from loom.streaming import ErrorEnvelope, Message, RecordStep

_logger = logging.getLogger(__name__)


class PrintErrorEnvelopeTask(RecordStep[ErrorEnvelope[Any], ErrorEnvelope[Any]]):
    """Log an error envelope and pass it through unchanged."""

    def execute(
        self,
        message: Message[ErrorEnvelope[Any]],
        **_kwargs: object,
    ) -> ErrorEnvelope[Any]:
        envelope = message.payload
        original = envelope.original_message
        original_kind = None
        original_request_id = None
        if original is not None:
            original_kind = getattr(original.payload, "kind", None)
            original_request_id = getattr(original.payload, "request_id", None)
        _logger.info(
            "error_envelope kind=%s reason=%s original_kind=%s original_request_id=%s",
            envelope.kind,
            envelope.reason,
            original_kind,
            original_request_id,
        )
        return envelope


class PrintBusinessErrorTask(RecordStep[ErrorEnvelope[Any], ErrorEnvelope[Any]]):
    """Log business errors with a distinct marker and pass them through."""

    def execute(
        self,
        message: Message[ErrorEnvelope[Any]],
        **_kwargs: object,
    ) -> ErrorEnvelope[Any]:
        envelope = message.payload
        original = envelope.original_message
        original_request_id = None
        if original is not None:
            original_request_id = getattr(original.payload, "request_id", None)
        _logger.info(
            "business_error_envelope reason=%s original_request_id=%s",
            envelope.reason,
            original_request_id,
        )
        return envelope
