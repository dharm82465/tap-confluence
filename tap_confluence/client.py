"""REST client handling, including ConfluenceStream base class."""

from __future__ import annotations

import decimal
import sys
from typing import TYPE_CHECKING, Any, ClassVar
import re
from html import unescape
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

from tap_confluence import schemas
from tap_confluence.html_utils import simplify_html
from typing import Any, Dict, Iterable, List
from urllib.parse import parse_qsl
from bs4 import BeautifulSoup

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = SchemaDirectory("./schemas")


class NextPageTokenPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        links = response.json().get("_links")
        next = links.get('next')
        if not next:
            return None
        next_url = f"{links.get('base')}{next}"
        return next_url

class ConfluenceStream(RESTStream):
    """Confluence stream class."""

    path = "/content/search"
    limit: int = 10
    expand: List[str] = []

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return self.config.get("api_url", "")

    @override
    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator(token=self.config.get("auth_token", ""))

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return {}

    @override
    def get_new_paginator(self) -> BaseAPIPaginator | None:
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
        cql = []
        if self.config.get("space_key"):
            cql.insert(0, f"space={self.config.get('space_key')}")
        if self.config.get("content_type"):
            cql.insert(0, f"type={self.config.get('content_type')}")
        if self.config.get("start_date"):
            cql.insert(0, f"lastmodified >= '{self.config.get('start_date')}'")    
        params = {
            "expand": ",".join(self.expand),
            "cql": f'{" AND ".join(cql)} ORDER BY lastmodified'
        }
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))
        return params

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        resp_json = response.json()
        for row in resp_json["results"]:
            yield row

    @staticmethod
    def remove_html_tags(html: str) -> str:
        """Remove HTML tags and decode HTML entities."""
        if not html:
            return html
        return simplify_html(BeautifulSoup(html, "html.parser"))

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
        # TODO: Delete this method if not needed.

        if row.get("body", {}).get("storage", {}).get("value") is not None:
            content = row.get("body", {}).get("storage", {}).get("value", "")
            row["body"]["storage"]["value"] = ConfluenceStream.remove_html_tags(content) 
          
        return row
