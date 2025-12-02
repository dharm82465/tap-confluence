"""Confluence tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_confluence.streams import (
    ContentStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

STREAM_TYPES = [
    ContentStream,
]


class TapConfluence(Tap):
    """Singer tap for Confluence."""

    name = "tap-confluence"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "base_url",
            th.StringType(nullable=False),
            required=True,
            title="BASE URL",
            default="https://api.mysample.com",
            description="Confluence instance base URL",
        ),
        th.Property(
            "space_keys",
            th.ArrayType(th.StringType, nullable=True),
            title="SPACE KEY",
            description="Space keys to sync",
        ),
        th.Property(
            "content_types",
            th.ArrayType(th.StringType, nullable=True),
            title="CONTENT TYPE",
            description="possible values are page, blogpost, comment, attachment",
        ),
        th.Property(
            "file_extensions",
            th.ArrayType(th.StringType, nullable=True),
            title="FILE EXTENSIONS",
            description="file extensions for attachments, e.g., pdf, docx, pptx",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            title="START DATE",
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.ConfluenceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream(tap=self) for stream in STREAM_TYPES]


if __name__ == "__main__":
    TapConfluence.cli()
