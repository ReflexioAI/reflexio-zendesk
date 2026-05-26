"""Shared timestamp parsing utility for Supabase storage converters."""

import re
from datetime import datetime


def _parse_iso_timestamp(ts: str) -> int:
    """
    Parse an ISO 8601 timestamp string to a Unix timestamp int.

    Handles variable-precision fractional seconds from Postgres (e.g. 5-digit microseconds)
    that Python 3.10's datetime.fromisoformat() cannot parse.

    Args:
        ts: ISO 8601 timestamp string

    Returns:
        int: Unix timestamp
    """
    ts = ts.replace("Z", "+00:00")
    # Normalize fractional seconds to exactly 6 digits for Python 3.10 compat
    ts = re.sub(
        r"\.(\d+)",
        lambda m: "." + m.group(1)[:6].ljust(6, "0"),
        ts,
    )
    return int(datetime.fromisoformat(ts).timestamp())
