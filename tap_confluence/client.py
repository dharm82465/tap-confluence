"""REST client handling, including ConfluenceStream base class."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl

from docling.document_converter import DocumentConverter, InputFormat
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream

from tap_confluence.attachment import AttachmentFetcher
from tap_confluence.paginator import NextPageTokenPaginator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Any, ClassVar, Iterable

    import requests
    from singer_sdk.helpers.types import Context
    from singer_sdk.pagination import BaseHATEOASPaginator
    from singer_sdk.tap_base import Tap
    from singer_sdk.typing import Schema

SCHEMAS_DIR = SchemaDirectory("./schemas")


class ConfluenceStream(RESTStream):
    """Confluence stream class."""

    path = "/content/search"
    limit: int = 10
    expand: list[str] = []

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    replication_method = "INCREMENTAL"
    replication_key = "_modified_time"
    primary_keys = ["id"]

    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        schema: dict[str, Any] | Schema | None = None,
        path: str | None = None,
        *,
        http_method: str | None = None,
    ) -> None:
        """Initialize the Confluence stream with document converter and attachment fetcher."""
        super().__init__(tap, name, schema, path, http_method=http_method)
        self.converter = DocumentConverter()
        self.attachment_fetcher = AttachmentFetcher(
            converter=self.converter,
            token=self.config.get("auth_token") or os.getenv("AUTH_TOKEN"),
        )

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"{self.config.get('base_url') or os.getenv('BASE_URL')}/rest/api"

    @override
    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator(
            token=self.config.get("auth_token") or os.getenv("AUTH_TOKEN")
        )

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {}

    @override
    def get_new_paginator(self) -> BaseHATEOASPaginator | None:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        return NextPageTokenPaginator()

    def __get_list_from_config_or_env(
        self,
        config_key: str,
        env_key: str,
    ) -> list[str]:
        """Get a list of strings from config or environment variable.

        Args:
            config_key: The configuration key to look for.
            env_key: The environment variable key to look for.
        Returns:
            A list of strings.
        """
        env_value = os.getenv(env_key)
        if env_value:
            return [item.strip() for item in env_value.split(",")]
        config_value = self.config.get(config_key)
        if config_value:
            return config_value
        return []

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        self.logger.info(
            "Replication Key Value: %s",
            self.get_starting_replication_key_value(context),
        )
        cql = []
        space_keys = self.__get_list_from_config_or_env("space_keys", "SPACE_KEYS")
        if len(space_keys) > 0:
            cql.append("space in (" + ",".join(space_keys) + ")")
        if self.get_starting_replication_key_value(context):
            start_date = self.get_starting_replication_key_value(context)
            cql.append(f"lastmodified > '{start_date}'")
        elif self.config.get("start_date"):
            start_date = self.config.get("start_date")
            cql.append(f"lastmodified > '{start_date}'")

        content_types = self.__get_list_from_config_or_env("content_types", "CONTENT_TYPES")
        file_extensions = self.__get_list_from_config_or_env("file_extensions", "FILE_EXTENSIONS")
        if len(content_types) > 0 and len(file_extensions) == 0:
            cql.append("type in (" + ",".join(content_types) + ")")
        elif len(content_types) == 0 and len(file_extensions) > 0:
            query = '((type != "attachment") OR (type = attachment AND ('
            query += " OR ".join(
                [f'sitesearch ~ "file.extension:{ext}"' for ext in file_extensions]
            )
            query += ")))"
            cql.append(query)
        elif len(content_types) > 0 and len(file_extensions) > 0:
            content_types = [x for x in content_types if x != "attachment"]
            query = "((type in (" + ",".join(content_types) + ")) OR ("
            query += "type = attachment AND ("
            query += " OR ".join(
                [f'sitesearch ~ "file.extension:{ext}"' for ext in file_extensions]
            )
            query += ")))"
            cql.append(query)
        params = {
            "expand": ",".join(self.expand),
            "cql": f"{' AND '.join(cql)} ORDER BY lastmodified",
        }
        self.logger.info("CQL: %s ORDER BY lastmodified", " AND ".join(cql))
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))
        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        resp_json = response.json()
        yield from resp_json["results"]

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row["_modified_time"] = datetime.fromisoformat(row.get("version").get("when")).strftime(
            "%Y-%m-%d %H:%M"
        )
        content_type = row.get("type")
        if content_type == "attachment":
            path = row.get("_links", {}).get("download")
            if not path:
                return None
            download_url = f"{self.config.get('base_url')}{path}"
            title = row.get("title", "")
            markdown = self.attachment_fetcher.fetch_attachment(url=download_url, title=title)
            if markdown is None or len(markdown) == 0:
                return None
            row["body"]["storage"]["value"] = markdown
            return row

        content = row.get("body", {}).get("storage", {}).get("value", "")
        if len(content) > 0:
            result = self.converter.convert_string(content, InputFormat.HTML)
            markdown = result.document.export_to_markdown()
            if len(markdown) > 0:
                row["body"]["storage"]["value"] = markdown
            else:
                return None
        return row
