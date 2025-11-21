"""Stream type classes for tap-confluence."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers
# from singer_sdk import SchemaDirectory, StreamSchema

from tap_confluence.client import ConfluenceStream

from pathlib import Path
# import abc

# TODO: - Override `UsersStream` and `GroupStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class ContentStream(ConfluenceStream):
    name = "content"
    primary_keys = ["id"]

    expand = [
        "history",
        "history.lastUpdated",
        "history.previousVersion",
        "history.contributors",
        "restrictions.read.restrictions.user",
        "version",
        "descendants.comment",
        "body.storage",
    ]
