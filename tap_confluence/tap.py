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

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
        th.Property(
            "space_key",
            th.StringType(nullable=True),
            description="The space key to sync",
        ),
        th.Property(
            "content_type",
            th.StringType(nullable=True),
            description="The content type to sync (either page or blogpost)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.ConfluenceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        # return [
        #     streams.GroupsStream(self),
        #     streams.UsersStream(self),
        # ]
        return [stream(tap=self) for stream in STREAM_TYPES]

if __name__ == "__main__":
    TapConfluence.cli()
