"""
Instantly.ai API Integration

Production-grade client for Instantly cold email platform.

Features:
- Automatic retries with exponential backoff
- Respects Retry-After headers
- Request timeouts
- Pagination iterators
- Structured logging
- Environment variable configuration
- Context manager support

Docs: https://developer.instantly.ai/
"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Set, Tuple

import pandas as pd
import requests

if TYPE_CHECKING:
    from outbound_engine import ABTestManager

# =========================================
# Logging
# =========================================

logger = logging.getLogger(__name__)

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
    """
    Configuration for Instantly API connection.

    Can be initialized from environment variables:
        config = InstantlyConfig.from_env()
    """

    api_key: str
    base_url: str = "https://api.instantly.ai/api/v1"
    timeout: Tuple[float, float] = DEFAULT_TIMEOUT
    max_retries: int = MAX_RETRIES

    @classmethod
    def from_env(cls) -> "InstantlyConfig":
        """
        Load configuration from environment variables.

        Environment variables:
            INSTANTLY_API_KEY: Required API key
            INSTANTLY_BASE_URL: Optional custom base URL
            INSTANTLY_TIMEOUT_CONNECT: Optional connect timeout (default: 3.05)
            INSTANTLY_TIMEOUT_READ: Optional read timeout (default: 30)
            INSTANTLY_MAX_RETRIES: Optional max retries (default: 3)
        """
        api_key = os.environ.get("INSTANTLY_API_KEY")
        if not api_key:
            raise ValueError("INSTANTLY_API_KEY environment variable is required")

        base_url = os.environ.get("INSTANTLY_BASE_URL", "https://api.instantly.ai/api/v1")

        connect_timeout = float(os.environ.get("INSTANTLY_TIMEOUT_CONNECT", "3.05"))
        read_timeout = float(os.environ.get("INSTANTLY_TIMEOUT_READ", "30"))

        max_retries = int(os.environ.get("INSTANTLY_MAX_RETRIES", "3"))

        return cls(
            api_key=api_key,
            base_url=base_url,
            timeout=(connect_timeout, read_timeout),
            max_retries=max_retries,
        )


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
    - Context manager support for proper cleanup

    Usage:
        # From API key
        client = InstantlyClient(api_key="your-key")

        # From environment
        client = InstantlyClient.from_env()

        # As context manager
        with InstantlyClient.from_env() as client:
            campaigns = client.list_campaigns()
    """

    def __init__(self, api_key: Optional[str] = None, config: Optional[InstantlyConfig] = None):
        """
        Initialize client with API key or config.

        Args:
            api_key: Instantly API key (ignored if config is provided)
            config: Full configuration object
        """
        if config:
            self.config = config
        elif api_key:
            self.config = InstantlyConfig(api_key=api_key)
        else:
            raise ValueError("Either api_key or config must be provided")

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

        logger.debug("InstantlyClient initialized with base_url=%s", self.config.base_url)

    @classmethod
    def from_env(cls) -> "InstantlyClient":
        """Create client from environment variables."""
        return cls(config=InstantlyConfig.from_env())

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()
        logger.debug("InstantlyClient session closed")

    def __enter__(self) -> "InstantlyClient":
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.close()

    # =========================================
    # Internal: Rate Limiting & Retries
    # =========================================

    def _rate_limit(self) -> None:
        """Ensure we don't exceed rate limits, respecting any backoff."""
        now = time.time()

        # If we got a 429 and were told to back off, wait
        if now < self._backoff_until:
            sleep_time = self._backoff_until - now
            logger.debug("Rate limit backoff: sleeping %.2fs", sleep_time)
            time.sleep(sleep_time)

        # Standard rate limiting
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)

        self._last_request_time = time.time()

    def _sleep_backoff(self, attempt: int, base: float = 0.5, cap: float = 10.0) -> None:
        """Sleep with exponential backoff + jitter."""
        delay = min(cap, base * (2**attempt)) + random.uniform(0, 0.25)
        logger.warning("Retry attempt %d: sleeping %.2fs", attempt + 1, delay)
        time.sleep(delay)

    def _handle_retry_after(self, response: requests.Response) -> None:
        """Parse Retry-After header and set backoff time."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = int(retry_after)
                self._backoff_until = time.time() + seconds
                logger.info("Retry-After header received: %ds", seconds)
            except ValueError:
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

            logger.debug("%s %s (attempt %d/%d)", method, endpoint, attempt + 1, self.config.max_retries)

            try:
                if method == "GET":
                    response = self.session.get(url, params=params, timeout=self.config.timeout)
                elif method == "POST":
                    response = self.session.post(url, params=params, json=json_data, timeout=self.config.timeout)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                logger.debug("%s %s -> %d", method, endpoint, response.status_code)

                # Check for retryable status codes
                if response.status_code in RETRYABLE_STATUS_CODES:
                    self._handle_retry_after(response)
                    if attempt < self.config.max_retries - 1:
                        self._sleep_backoff(attempt)
                        continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout as e:
                logger.warning("%s %s timed out", method, endpoint)
                last_exception = e
                if attempt < self.config.max_retries - 1:
                    self._sleep_backoff(attempt)
                    continue

            except requests.exceptions.HTTPError as e:
                payload = None
                try:
                    payload = response.json()
                except (ValueError, AttributeError):
                    pass

                logger.error("%s %s failed: %s", method, endpoint, e)
                raise InstantlyAPIError(str(e), status_code=response.status_code, payload=payload)

            except requests.exceptions.RequestException as e:
                logger.warning("%s %s request error: %s", method, endpoint, e)
                last_exception = e
                if attempt < self.config.max_retries - 1:
                    self._sleep_backoff(attempt)
                    continue

        logger.error("Request failed after %d retries: %s", self.config.max_retries, last_exception)
        raise InstantlyAPIError(f"Request failed after {self.config.max_retries} retries: {last_exception}")

    def _extract_list(self, data: Dict | List, key: str) -> List[Dict]:
        """Extract list from API response."""
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
        """Generic pagination iterator."""
        skip = 0
        total_yielded = 0

        while True:
            params = {**base_params, "skip": skip, "limit": page_size}
            data = self._request("GET", endpoint, params=params)
            items = self._extract_list(data, item_key)

            if not items:
                break

            yield from items
            total_yielded += len(items)

            skip += len(items)
            if len(items) < page_size:
                break

        logger.debug("Paginated %s: yielded %d items", endpoint, total_yielded)

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
        """Add leads to a campaign."""
        logger.info("Adding %d leads to campaign %s", len(leads), campaign_id)
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
        """Get campaign-level analytics."""
        return self._request("GET", "analytics/campaign/summary", params={"campaign_id": campaign_id})

    def get_lead_activity(
        self,
        campaign_id: str,
        email: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict]:
        """Get activity/events for leads."""
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


@dataclass
class SyncCheckpoint:
    """Tracks sync state for a campaign."""

    campaign_id: str
    last_sync_at: Optional[datetime] = None
    seen_opens: Set[str] = field(default_factory=set)  # Set of emails
    seen_replies: Set[str] = field(default_factory=set)  # Set of emails


class InstantlySync:
    """
    Syncs Instantly engagement data with local A/B test tracking.

    Features:
    - Deduplicates opens/replies per email
    - Tracks sync checkpoints per campaign
    - Actually updates ABTestManager records
    - Graceful error handling
    """

    def __init__(self, instantly_client: InstantlyClient, ab_manager: "ABTestManager"):
        self.instantly = instantly_client
        self.ab_manager = ab_manager
        self._checkpoints: Dict[str, SyncCheckpoint] = {}

    def _get_checkpoint(self, campaign_id: str) -> SyncCheckpoint:
        """Get or create sync checkpoint for a campaign."""
        if campaign_id not in self._checkpoints:
            self._checkpoints[campaign_id] = SyncCheckpoint(campaign_id=campaign_id)
        return self._checkpoints[campaign_id]

    def sync_campaign_results(
        self,
        campaign_id: str,
        variant_mapping: Dict[str, str],
    ) -> Dict:
        """
        Pull engagement from Instantly and update A/B test stats.

        Args:
            campaign_id: Instantly campaign ID
            variant_mapping: Dict mapping lead email -> send_id (for ABTestManager)

        Returns:
            Sync summary with counts and any errors
        """
        checkpoint = self._get_checkpoint(campaign_id)
        summary: Dict = {
            "opens_synced": 0,
            "opens_skipped": 0,
            "replies_synced": 0,
            "replies_skipped": 0,
            "errors": [],
        }

        # Sync opens
        try:
            opens = self.instantly.get_lead_activity(campaign_id, event_type="opened")
            for event in opens:
                email = event.get("email")
                if not email or email not in variant_mapping:
                    continue

                if email in checkpoint.seen_opens:
                    summary["opens_skipped"] += 1
                    continue

                # Record open in ABTestManager
                send_id = variant_mapping[email]
                try:
                    self.ab_manager.record_open(send_id)
                    checkpoint.seen_opens.add(email)
                    summary["opens_synced"] += 1
                except Exception as e:
                    logger.warning("Failed to record open for %s: %s", email, e)

        except InstantlyAPIError as e:
            logger.error("Failed to fetch opens for campaign %s: %s", campaign_id, e)
            summary["errors"].append(f"Failed to fetch opens: {e.message}")

        # Sync replies
        try:
            for reply in self.instantly.iter_replies(campaign_id):
                email = reply.get("email")
                if not email or email not in variant_mapping:
                    continue

                if email in checkpoint.seen_replies:
                    summary["replies_skipped"] += 1
                    continue

                # Record reply in ABTestManager
                send_id = variant_mapping[email]
                # Determine sentiment if available
                sentiment = reply.get("sentiment", "neutral")
                try:
                    self.ab_manager.record_reply(send_id, sentiment=sentiment)
                    checkpoint.seen_replies.add(email)
                    summary["replies_synced"] += 1
                except Exception as e:
                    logger.warning("Failed to record reply for %s: %s", email, e)

        except InstantlyAPIError as e:
            logger.error("Failed to fetch replies for campaign %s: %s", campaign_id, e)
            summary["errors"].append(f"Failed to fetch replies: {e.message}")

        # Update checkpoint timestamp
        checkpoint.last_sync_at = datetime.now(timezone.utc)

        logger.info(
            "Sync complete for campaign %s: %d opens, %d replies synced",
            campaign_id,
            summary["opens_synced"],
            summary["replies_synced"],
        )

        return summary

    def get_sync_status(self, campaign_id: str) -> Dict:
        """Get sync status for a campaign."""
        checkpoint = self._get_checkpoint(campaign_id)
        return {
            "campaign_id": campaign_id,
            "last_sync_at": checkpoint.last_sync_at.isoformat() if checkpoint.last_sync_at else None,
            "unique_opens_seen": len(checkpoint.seen_opens),
            "unique_replies_seen": len(checkpoint.seen_replies),
        }

    def reset_checkpoint(self, campaign_id: str) -> None:
        """Reset sync checkpoint for a campaign (forces full re-sync)."""
        if campaign_id in self._checkpoints:
            del self._checkpoints[campaign_id]
            logger.info("Reset sync checkpoint for campaign %s", campaign_id)

    def get_account_health(self) -> pd.DataFrame:
        """Get health status of all email accounts."""
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
                logger.warning("Failed to get status for account %s: %s", email, e)
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
# CLI
# =========================================


def _cli():
    """Simple CLI for smoke testing."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Instantly.ai API client")
    parser.add_argument("command", choices=["list-campaigns", "list-accounts", "health", "smoke-test"])
    parser.add_argument("--api-key", help="API key (or set INSTANTLY_API_KEY)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    # Initialize client
    try:
        if args.api_key:
            client = InstantlyClient(api_key=args.api_key)
        else:
            client = InstantlyClient.from_env()
    except ValueError as e:
        print(f"Error: {e}")
        print("Set INSTANTLY_API_KEY environment variable or use --api-key")
        return 1

    with client:
        try:
            if args.command == "list-campaigns":
                campaigns = client.list_campaigns()
                print(json.dumps(campaigns, indent=2))

            elif args.command == "list-accounts":
                accounts = client.list_accounts()
                print(json.dumps(accounts, indent=2))

            elif args.command == "health":
                # Need to create a minimal sync helper
                class MockABManager:
                    pass

                sync = InstantlySync(client, MockABManager())
                df = sync.get_account_health()
                print(df.to_string())

            elif args.command == "smoke-test":
                print("Running smoke test...")
                print(f"  Base URL: {client.config.base_url}")
                print(f"  Timeout: {client.config.timeout}")
                print(f"  Max retries: {client.config.max_retries}")

                campaigns = client.list_campaigns(limit=1)
                print(f"  Campaigns accessible: {len(campaigns)} found")

                accounts = client.list_accounts()
                print(f"  Accounts connected: {len(accounts)}")

                print("Smoke test passed!")

        except InstantlyAPIError as e:
            print(f"API Error: {e}")
            if e.payload:
                print(f"Payload: {json.dumps(e.payload, indent=2)}")
            return 1

    return 0


if __name__ == "__main__":
    exit(_cli())
