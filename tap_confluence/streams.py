"""Stream type classes for tap-confluence."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers
# from singer_sdk import SchemaDirectory, StreamSchema

from tap_confluence.client import ConfluenceStream

from pathlib import Path
# import abc

# TODO: - Override `UsersStream` and `GroupStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

class GroupStream(ConfluenceStream):
    name = "group"
    path = "/group"
    primary_keys = ["id"]

class SpaceStream(ConfluenceStream):
    name = "space"
    path = "/space"
    primary_keys = ["id"]

    expand = [
        "icon",
        "description.plain",
        "description.view",
        "homepage.body.storage",
    ]


class ThemeStream(ConfluenceStream):
    name = "theme"
    path = "/settings/theme"
    primary_keys = ["themeKey"]

    expand = [
        "icon",
    ]

class BlogpostStream(ConfluenceStream):
    name = "blogpost"
    path = "/content"
    primary_keys = ["id"]

    expand = [
        "history",
        "history.lastUpdated",
        "history.previousVersion",
        "history.contributors",
        "restrictions.read.restrictions.user",
        "version",
        "descendants.comment",
    ]

    def get_url_params(
        self,
        partition: dict | None,
        next_page_token: int | None,
    ) -> Dict[str, Any]:
        result = super().get_url_params(partition, next_page_token)
        result["type"] = "blogpost"
        return result


class PageStream(ConfluenceStream):
    name = "page"
    path = "/content"
    primary_keys = ["id"]

    expand = [
        "history",
        "history.lastUpdated",
        "history.previousVersion",
        "history.contributors",
        "restrictions.read.restrictions.user",
        "version",
        "descendants.comment",
    ]

    
    def get_url_params(
        self,
        partition: dict | None,
        next_page_token: int | None,
    ) -> Dict[str, Any]:
        result = super().get_url_params(partition, next_page_token)
        result["type"] = "page"
        return result