"""Request models validating gateway input before any job is created.

Invalid requests therefore never reach Redis or RabbitMQ. Validation limits
are named below; the shared interface is documented in docs/INTERFACES.md.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
import re
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


HOST_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
MAX_NAME_LENGTH = 255


def _clean_text(value: str, field_name: str, max_length: int | None = None) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty")
    if max_length is not None and len(cleaned) > max_length:
        raise ValueError(f"{field_name} is too long")
    return cleaned


def _normalize_timestamp(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string")

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be an ISO-8601 string") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    normalized = parsed.astimezone(timezone.utc)
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


class MeasurementIn(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
    )

    source_host: str
    target_host: str
    metric: str
    values_ms: list[int | float] = Field(
        validation_alias=AliasChoices("values_ms", "values"),
        min_length=1,
    )
    unit: str | None = None
    timestamp: str | None = None
    packets_sent: int | None = None
    packets_received: int | None = None
    analysis: str | None = None
    threshold_ms: int | float | None = None

    @field_validator("source_host", "target_host", mode="before")
    @classmethod
    def validate_host(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("host must be a string")
        cleaned = _clean_text(value, "host", MAX_NAME_LENGTH)
        # Hosts become object-path components, so slashes and spaces are invalid.
        if not HOST_PATTERN.fullmatch(cleaned):
            raise ValueError("host contains invalid characters")
        return cleaned

    @field_validator("metric", mode="before")
    @classmethod
    def validate_metric(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("metric must be a string")
        cleaned = _clean_text(value, "metric", MAX_NAME_LENGTH)
        if any(character.isspace() and ord(character) < 32 for character in cleaned):
            raise ValueError("metric contains control characters")
        if any(ord(character) < 32 or ord(character) == 127 for character in cleaned):
            raise ValueError("metric contains control characters")
        return cleaned

    @field_validator("values_ms", mode="before")
    @classmethod
    def validate_values(cls, value: Any) -> Any:
        if not isinstance(value, list) or not value:
            raise ValueError("values_ms must contain at least one sample")
        for sample in value:
            if isinstance(sample, bool) or not isinstance(sample, (int, float)):
                raise ValueError("values_ms samples must be numbers")
            if not math.isfinite(sample) or sample < 0:
                raise ValueError("values_ms samples must be finite and non-negative")
        return value

    @field_validator("unit", "analysis", mode="before")
    @classmethod
    def validate_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        return _clean_text(value, "value")

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, value: Any) -> str | None:
        if value is None:
            return None
        return _normalize_timestamp(value)

    @field_validator("packets_sent", "packets_received", mode="before")
    @classmethod
    def validate_packet_count(cls, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("packet counts must be non-negative integers")
        return value

    @field_validator("threshold_ms", mode="before")
    @classmethod
    def validate_threshold(cls, value: Any) -> int | float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("threshold_ms must be a number")
        if not math.isfinite(value) or value <= 0:
            raise ValueError("threshold_ms must be finite and positive")
        return value

    def model_post_init(self, __context: Any) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            )

    def queue_dict(self) -> dict[str, Any]:
        """Return the normalized, non-null snake_case queue payload."""
        return self.model_dump(exclude_none=True)


class SubmitAnalyzeIn(MeasurementIn):
    analysis: str


class AnalyzeStoredIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    analysis: str
    threshold_ms: int | float | None = None

    @field_validator("analysis", mode="before")
    @classmethod
    def validate_analysis(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("analysis must be a string")
        return _clean_text(value, "analysis")

    @field_validator("threshold_ms", mode="before")
    @classmethod
    def validate_threshold(cls, value: Any) -> int | float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("threshold_ms must be a number")
        if not math.isfinite(value) or value <= 0:
            raise ValueError("threshold_ms must be finite and positive")
        return value
