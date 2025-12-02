"""Stream type classes for tap-confluence."""

from __future__ import annotations

from tap_confluence.client import ConfluenceStream


class ContentStream(ConfluenceStream):
    """Stream for retrieving content from Confluence."""

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
