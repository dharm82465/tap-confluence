"""Pagination classes for the Confluence tap.

This module provides:
- NextPageTokenPaginator: paginator that extracts next page URL from response links.
"""

import requests
from singer_sdk.pagination import BaseHATEOASPaginator


class NextPageTokenPaginator(BaseHATEOASPaginator):
    """Paginator that extracts next page URL from response links."""

    def get_next_url(self, response: requests.Response) -> str | None:
        """Extract the next page URL from the response links.

        Args:
            response: The HTTP response object.

        Returns:
            The next page URL if available, otherwise None.
        """
        links = response.json().get("_links")
        next_result = links.get("next")
        if not next_result:
            return None
        return f"{links.get('base')}{next_result}"
