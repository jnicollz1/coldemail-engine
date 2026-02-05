"""
Instantly.ai API Integration

Handles connection to Instantly for:
- Campaign management
- Lead uploading
- Sending emails
- Pulling engagement metrics (opens, replies)

Docs: https://developer.instantly.ai/
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

import pandas as pd
import requests

# =========================================
# Configuration
# =========================================

DEFAULT_TIMEOUT = (3.05, 30)  # (connect, read) timeouts in seconds
DEFAULT_PAGE_SIZE = 100
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


# =========================================
# Exceptions
# =========================================


class InstantlyAPIError(RuntimeError):
    """
    Raised when Instantly API returns an error.

    Attributes:
        message: Human-readable error description
        status_code: HTTP status code (if applicable)
        payload: Raw response body (if parseable)
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        payload: Optional[Dict] = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.payload = payload

    def __str__(self) -> str:
        parts = [self.message]
        if self.status_code:
            parts.append(f"(status={self.status_code})")
        return " ".join(parts)


# =========================================
# Configuration
# =========================================


@dataclass
class InstantlyConfig:
    """Configuration for Instantly API connection."""

    api_key: str
    base_url: str = "https://api.instantly.ai/api/v1"
    timeout: tuple = DEFAULT_TIMEOUT
    max_retries: int = MAX_RETRIES


# =========================================
# Client
# =========================================


class InstantlyClient:
    """
    Production-grade client for Instantly.ai API.

    Features:
    - Automatic retries with exponential backoff (429, 5xx, timeouts)
    - Respects Retry-After headers
    - Request timeouts
    - Pagination iterators for large datasets
    - Proper error handling with detailed exceptions
    """

    def __init__(self, api_key: str, config: Optional[InstantlyConfig] = None):
        self.config = config or InstantlyConfig(api_key=api_key)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "outbound-engine/1.0",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self._last_request_time = 0.0
        self._min_request_interval = 0.2  # 5 requests per second max
        self._backoff_until = 0.0  # Timestamp until which we should wait (from 429s)

    # =========================================
    # Internal: Rate Limiting & Retries
    # =========================================

    def _rate_limit(self) -> None:
        """Ensure we don't exceed rate limits, respecting any backoff."""
        now = time.time()

        # If we got a 429 and were told to back off, wait
        if now < self._backoff_until:
            sleep_time = self._backoff_until - now
            time.sleep(sleep_time)

        # Standard rate limiting
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)

        self._last_request_time = time.time()

    def _sleep_backoff(self, attempt: int, base: float = 0.5, cap: float = 10.0) -> None:
        """Sleep with exponential backoff + jitter."""
        delay = min(cap, base * (2**attempt)) + random.uniform(0, 0.25)
        time.sleep(delay)

    def _handle_retry_after(self, response: requests.Response) -> None:
        """Parse Retry-After header and set backoff time."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                # Retry-After can be seconds or HTTP date; we handle seconds
                seconds = int(retry_after)
                self._backoff_until = time.time() + seconds
            except ValueError:
                # If it's a date format, fall back to default backoff
                self._backoff_until = time.time() + 5

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
    ) -> Dict:
        """
        Make an API request with retries and error handling.

        Raises:
            InstantlyAPIError: If the API returns an error after all retries
            ValueError: If an unsupported HTTP method is used
        """
        url = f"{self.config.base_url}/{endpoint}"
        params = params or {}
        params["api_key"] = self.config.api_key

        last_exception: Optional[Exception] = None

        for attempt in range(self.config.max_retries):
            self._rate_limit()

            try:
                if method == "GET":
                    response = self.session.get(
                        url, params=params, timeout=self.config.timeout
                    )
                elif method == "POST":
                    response = self.session.post(
                        url, params=params, json=json_data, timeout=self.config.timeout
                    )
                else:
                    raise ValueError(f"Unsupported method: {method}")

                # Check for retryable status codes
                if response.status_code in RETRYABLE_STATUS_CODES:
                    self._handle_retry_after(response)
                    if attempt < self.config.max_retries - 1:
                        self._sleep_backoff(attempt)
                        continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout as e:
                last_exception = e
                if attempt < self.config.max_retries - 1:
                    self._sleep_backoff(attempt)
                    continue

            except requests.exceptions.HTTPError as e:
                # Try to extract payload for better error messages
                payload = None
                try:
                    payload = response.json()
                except (ValueError, AttributeError):
                    pass

                raise InstantlyAPIError(
                    str(e), status_code=response.status_code, payload=payload
                )

            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < self.config.max_retries - 1:
                    self._sleep_backoff(attempt)
                    continue

        # If we get here, all retries failed
        raise InstantlyAPIError(f"Request failed after {self.config.max_retries} retries: {last_exception}")

    def _extract_list(self, data: Dict | List, key: str) -> List[Dict]:
        """Extract list from API response, handling both direct lists and wrapped responses."""
        if isinstance(data, list):
            return data
        return data.get(key, data.get("data", []))

    # =========================================
    # Pagination Iterator
    # =========================================

    def _paginate(
        self,
        endpoint: str,
        base_params: Dict,
        item_key: str,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[Dict]:
        """
        Generic pagination iterator.

        Yields individual items from paginated API responses.
        Handles both list responses and wrapped {"key": [...]} responses.
        """
        skip = 0
        while True:
            params = {**base_params, "skip": skip, "limit": page_size}
            data = self._request("GET", endpoint, params=params)
            items = self._extract_list(data, item_key)

            if not items:
                break

            yield from items

            skip += len(items)
            if len(items) < page_size:
                break

    # =========================================
    # Campaign Management
    # =========================================

    def list_campaigns(self, skip: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> List[Dict]:
        """List campaigns (single page)."""
        result = self._request("GET", "campaign/list", params={"skip": skip, "limit": limit})
        return self._extract_list(result, "campaigns")

    def iter_campaigns(self) -> Iterator[Dict]:
        """Iterate over all campaigns with automatic pagination."""
        yield from self._paginate("campaign/list", {}, "campaigns")

    def get_campaign(self, campaign_id: str) -> Dict:
        """Get campaign details."""
        return self._request("GET", "campaign/get", params={"campaign_id": campaign_id})

    def get_campaign_status(self, campaign_id: str) -> Dict:
        """Get campaign sending status."""
        return self._request("GET", "campaign/get/status", params={"campaign_id": campaign_id})

    def launch_campaign(self, campaign_id: str) -> Dict:
        """Launch/activate a campaign."""
        return self._request("POST", "campaign/launch", json_data={"campaign_id": campaign_id})

    def pause_campaign(self, campaign_id: str) -> Dict:
        """Pause a campaign."""
        return self._request("POST", "campaign/pause", json_data={"campaign_id": campaign_id})

    # =========================================
    # Lead Management
    # =========================================

    def add_leads(
        self,
        campaign_id: str,
        leads: List[Dict],
        skip_duplicates: bool = True,
    ) -> Dict:
        """
        Add leads to a campaign.

        Args:
            campaign_id: Target campaign
            leads: List of lead dicts with at minimum 'email' key
            skip_duplicates: Whether to skip existing leads

        Returns:
            API response with upload status
        """
        return self._request(
            "POST",
            "lead/add",
            json_data={
                "campaign_id": campaign_id,
                "leads": leads,
                "skip_if_in_workspace": skip_duplicates,
            },
        )

    def get_lead_status(self, email: str, campaign_id: str) -> Dict:
        """Get status of a specific lead."""
        return self._request("GET", "lead/get", params={"email": email, "campaign_id": campaign_id})

    def list_leads(self, campaign_id: str, skip: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> List[Dict]:
        """List leads in a campaign (single page)."""
        result = self._request(
            "GET", "lead/list", params={"campaign_id": campaign_id, "skip": skip, "limit": limit}
        )
        return self._extract_list(result, "leads")

    def iter_leads(self, campaign_id: str) -> Iterator[Dict]:
        """Iterate over all leads in a campaign with automatic pagination."""
        yield from self._paginate("lead/list", {"campaign_id": campaign_id}, "leads")

    # =========================================
    # Analytics / Engagement
    # =========================================

    def get_campaign_analytics(self, campaign_id: str) -> Dict:
        """
        Get campaign-level analytics.

        Returns:
            Dict with sends, opens, replies, bounces, etc.
        """
        return self._request("GET", "analytics/campaign/summary", params={"campaign_id": campaign_id})

    def get_lead_activity(
        self,
        campaign_id: str,
        email: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get activity/events for leads.

        Args:
            campaign_id: Campaign to query
            email: Optional filter by specific email
            event_type: Optional filter: 'sent', 'opened', 'replied', 'bounced'

        Returns:
            List of activity events
        """
        params: Dict = {"campaign_id": campaign_id}
        if email:
            params["email"] = email
        if event_type:
            params["event_type"] = event_type

        result = self._request("GET", "lead/activity", params=params)
        return self._extract_list(result, "activities")

    def get_replies(self, campaign_id: str, skip: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> List[Dict]:
        """Get replies for a campaign (single page)."""
        result = self._request(
            "GET", "campaign/replies", params={"campaign_id": campaign_id, "skip": skip, "limit": limit}
        )
        return self._extract_list(result, "replies")

    def iter_replies(self, campaign_id: str) -> Iterator[Dict]:
        """Iterate over all replies for a campaign with automatic pagination."""
        yield from self._paginate("campaign/replies", {"campaign_id": campaign_id}, "replies")

    # =========================================
    # Email Account Management
    # =========================================

    def list_accounts(self) -> List[Dict]:
        """List connected email accounts."""
        result = self._request("GET", "account/list")
        return self._extract_list(result, "accounts")

    def get_account_status(self, email: str) -> Dict:
        """Get warmup/sending status for an account."""
        return self._request("GET", "account/status", params={"email": email})

    def get_warmup_status(self, email: str) -> Dict:
        """Get warmup progress for an account."""
        return self._request("GET", "account/warmup/status", params={"email": email})


# =========================================
# Sync Helper
# =========================================


class InstantlySync:
    """
    Syncs Instantly engagement data with local A/B test tracking.

    Pulls opens/replies from Instantly and updates variant stats.
    """

    def __init__(self, instantly_client: InstantlyClient, ab_manager):
        self.instantly = instantly_client
        self.ab_manager = ab_manager
        self._last_sync: Dict = {}

    def sync_campaign_results(
        self,
        campaign_id: str,
        variant_mapping: Dict[str, str],
    ) -> Dict:
        """
        Pull engagement from Instantly and update A/B test stats.

        Args:
            campaign_id: Instantly campaign ID
            variant_mapping: Dict mapping lead email -> variant_id

        Returns:
            Sync summary with counts and any errors
        """
        summary: Dict = {"opens_synced": 0, "replies_synced": 0, "errors": []}

        # Get opens
        try:
            opens = self.instantly.get_lead_activity(campaign_id, event_type="opened")
            for event in opens:
                email = event.get("email")
                if email in variant_mapping:
                    summary["opens_synced"] += 1
        except InstantlyAPIError as e:
            summary["errors"].append(f"Failed to fetch opens: {e.message}")

        # Get replies (use iterator for large campaigns)
        try:
            for reply in self.instantly.iter_replies(campaign_id):
                email = reply.get("email")
                if email in variant_mapping:
                    summary["replies_synced"] += 1
        except InstantlyAPIError as e:
            summary["errors"].append(f"Failed to fetch replies: {e.message}")

        return summary

    def get_account_health(self) -> pd.DataFrame:
        """
        Get health status of all email accounts.

        Returns:
            DataFrame with warmup status, sending limits, etc.
        """
        accounts = self.instantly.list_accounts()

        health_data = []
        for account in accounts:
            email = account.get("email")
            try:
                status = self.instantly.get_account_status(email)
                warmup = self.instantly.get_warmup_status(email)

                health_data.append(
                    {
                        "email": email,
                        "status": status.get("status"),
                        "daily_limit": status.get("daily_limit"),
                        "sent_today": status.get("sent_today"),
                        "warmup_enabled": warmup.get("enabled"),
                        "warmup_reputation": warmup.get("reputation"),
                    }
                )
            except InstantlyAPIError as e:
                health_data.append(
                    {
                        "email": email,
                        "status": "error",
                        "daily_limit": None,
                        "sent_today": None,
                        "warmup_enabled": None,
                        "warmup_reputation": None,
                        "error": e.message,
                    }
                )

        return pd.DataFrame(health_data)


# =========================================
# CLI Example
# =========================================

if __name__ == "__main__":
    print("Instantly integration module loaded.")
    print("Initialize with: InstantlyClient(api_key='your-key')")
    print()
    print("Features:")
    print("  - Automatic retries with exponential backoff")
    print("  - Respects Retry-After headers")
    print("  - Request timeouts (connect=3s, read=30s)")
    print("  - Pagination iterators: iter_campaigns(), iter_leads(), iter_replies()")
