"""
Instantly.ai API Integration

Handles connection to Instantly for:
- Campaign management
- Lead uploading
- Sending emails
- Pulling engagement metrics (opens, replies)

Docs: https://developer.instantly.ai/
"""

import requests
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import time


class InstantlyAPIError(Exception):
    """Raised when Instantly API returns an error."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


@dataclass
class InstantlyConfig:
    """Configuration for Instantly API connection."""
    api_key: str
    base_url: str = "https://api.instantly.ai/api/v1"
    

class InstantlyClient:
    """
    Client for Instantly.ai API.
    
    Handles rate limiting and pagination automatically.
    """
    
    def __init__(self, api_key: str):
        self.config = InstantlyConfig(api_key=api_key)
        self.session = requests.Session()
        self._last_request_time = 0
        self._min_request_interval = 0.2  # 5 requests per second max
    
    def _rate_limit(self):
        """Ensure we don't exceed rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()
    
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Dict:
        """
        Make an API request with error handling.

        Raises:
            InstantlyAPIError: If the API returns an error response
        """
        self._rate_limit()

        url = f"{self.config.base_url}/{endpoint}"
        params = params or {}
        params["api_key"] = self.config.api_key

        try:
            if method == "GET":
                response = self.session.get(url, params=params)
            elif method == "POST":
                response = self.session.post(url, params=params, json=json_data)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            raise InstantlyAPIError(str(e), status_code=response.status_code)
        except requests.exceptions.RequestException as e:
            raise InstantlyAPIError(str(e))
    
    # =========================================
    # Campaign Management
    # =========================================
    
    def list_campaigns(self, skip: int = 0, limit: int = 100) -> List[Dict]:
        """List all campaigns."""
        result = self._request("GET", "campaign/list", params={
            "skip": skip,
            "limit": limit
        })
        # API returns list directly or wrapped in a key
        return result if isinstance(result, list) else result.get("campaigns", [])
    
    def get_campaign(self, campaign_id: str) -> Dict:
        """Get campaign details."""
        return self._request("GET", "campaign/get", params={
            "campaign_id": campaign_id
        })
    
    def get_campaign_status(self, campaign_id: str) -> Dict:
        """Get campaign sending status."""
        return self._request("GET", "campaign/get/status", params={
            "campaign_id": campaign_id
        })
    
    def launch_campaign(self, campaign_id: str) -> Dict:
        """Launch/activate a campaign."""
        return self._request("POST", "campaign/launch", json_data={
            "campaign_id": campaign_id
        })
    
    def pause_campaign(self, campaign_id: str) -> Dict:
        """Pause a campaign."""
        return self._request("POST", "campaign/pause", json_data={
            "campaign_id": campaign_id
        })
    
    # =========================================
    # Lead Management
    # =========================================
    
    def add_leads(
        self, 
        campaign_id: str, 
        leads: List[Dict],
        skip_duplicates: bool = True
    ) -> Dict:
        """
        Add leads to a campaign.
        
        Args:
            campaign_id: Target campaign
            leads: List of lead dicts with at minimum 'email' key
                   Can also include: first_name, last_name, company, etc.
            skip_duplicates: Whether to skip existing leads
        
        Returns:
            API response with upload status
        """
        return self._request("POST", "lead/add", json_data={
            "campaign_id": campaign_id,
            "leads": leads,
            "skip_if_in_workspace": skip_duplicates
        })
    
    def get_lead_status(self, email: str, campaign_id: str) -> Dict:
        """Get status of a specific lead."""
        return self._request("GET", "lead/get", params={
            "email": email,
            "campaign_id": campaign_id
        })
    
    def list_leads(
        self,
        campaign_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict]:
        """List leads in a campaign."""
        result = self._request("GET", "lead/list", params={
            "campaign_id": campaign_id,
            "skip": skip,
            "limit": limit
        })
        return result if isinstance(result, list) else result.get("leads", [])
    
    # =========================================
    # Analytics / Engagement
    # =========================================
    
    def get_campaign_analytics(self, campaign_id: str) -> Dict:
        """
        Get campaign-level analytics.
        
        Returns:
            Dict with sends, opens, replies, bounces, etc.
        """
        return self._request("GET", "analytics/campaign/summary", params={
            "campaign_id": campaign_id
        })
    
    def get_lead_activity(
        self,
        campaign_id: str,
        email: Optional[str] = None,
        event_type: Optional[str] = None
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
        params = {"campaign_id": campaign_id}
        if email:
            params["email"] = email
        if event_type:
            params["event_type"] = event_type

        result = self._request("GET", "lead/activity", params=params)
        return result if isinstance(result, list) else result.get("activities", [])
    
    def get_replies(
        self,
        campaign_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict]:
        """Get all replies for a campaign."""
        result = self._request("GET", "campaign/replies", params={
            "campaign_id": campaign_id,
            "skip": skip,
            "limit": limit
        })
        return result if isinstance(result, list) else result.get("replies", [])
    
    # =========================================
    # Email Account Management
    # =========================================
    
    def list_accounts(self) -> List[Dict]:
        """List connected email accounts."""
        result = self._request("GET", "account/list")
        return result if isinstance(result, list) else result.get("accounts", [])
    
    def get_account_status(self, email: str) -> Dict:
        """Get warmup/sending status for an account."""
        return self._request("GET", "account/status", params={
            "email": email
        })
    
    def get_warmup_status(self, email: str) -> Dict:
        """Get warmup progress for an account."""
        return self._request("GET", "account/warmup/status", params={
            "email": email
        })


class InstantlySync:
    """
    Syncs Instantly engagement data with local A/B test tracking.
    
    Pulls opens/replies from Instantly and updates variant stats.
    """
    
    def __init__(self, instantly_client: InstantlyClient, ab_manager):
        self.instantly = instantly_client
        self.ab_manager = ab_manager
        self._last_sync = {}
    
    def sync_campaign_results(
        self,
        campaign_id: str,
        variant_mapping: Dict[str, str]
    ) -> Dict:
        """
        Pull engagement from Instantly and update A/B test stats.

        Args:
            campaign_id: Instantly campaign ID
            variant_mapping: Dict mapping lead email -> variant_id

        Returns:
            Sync summary with counts

        Raises:
            InstantlyAPIError: If API calls fail
        """
        summary = {"opens_synced": 0, "replies_synced": 0, "errors": []}

        # Get opens
        try:
            opens = self.instantly.get_lead_activity(
                campaign_id,
                event_type="opened"
            )
            for event in opens:
                email = event.get("email")
                if email in variant_mapping:
                    summary["opens_synced"] += 1
        except InstantlyAPIError as e:
            summary["errors"].append(f"Failed to fetch opens: {e.message}")

        # Get replies
        try:
            replies = self.instantly.get_replies(campaign_id)
            for reply in replies:
                email = reply.get("email")
                if email in variant_mapping:
                    summary["replies_synced"] += 1
        except InstantlyAPIError as e:
            summary["errors"].append(f"Failed to fetch replies: {e.message}")

        return summary
    
    def get_account_health(self) -> pd.DataFrame:
        """
        Get health status of all email accounts.

        Returns DataFrame with warmup status, sending limits, etc.
        """
        accounts = self.instantly.list_accounts()
        
        health_data = []
        for account in accounts:
            email = account.get("email")
            status = self.instantly.get_account_status(email)
            warmup = self.instantly.get_warmup_status(email)
            
            health_data.append({
                "email": email,
                "status": status.get("status"),
                "daily_limit": status.get("daily_limit"),
                "sent_today": status.get("sent_today"),
                "warmup_enabled": warmup.get("enabled"),
                "warmup_reputation": warmup.get("reputation")
            })
        
        return pd.DataFrame(health_data)


# Example usage
if __name__ == "__main__":
    # This would use your actual API key
    # client = InstantlyClient(api_key="your-instantly-api-key")
    # campaigns = client.list_campaigns()
    
    print("Instantly integration module loaded.")
    print("Initialize with: InstantlyClient(api_key='your-key')")
