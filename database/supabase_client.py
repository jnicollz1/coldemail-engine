"""
Supabase Database Client for Outbound Engine

Provides secure, hosted database storage for:
- Prospect/lead data (PII stored securely off-device)
- A/B test configurations and results
- Email send tracking and engagement metrics

Why Supabase?
- SOC 2 compliant hosting
- Row-level security for data isolation
- Real-time subscriptions for live dashboards
- Built-in auth if needed for multi-user scenarios
"""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
import json

from supabase import create_client, Client


@dataclass
class DatabaseConfig:
    """Supabase connection configuration."""
    url: str
    key: str  # anon/public key for client-side, service key for server-side

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Load config from environment variables."""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError(
                "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY environment variables."
            )

        return cls(url=url, key=key)


class SupabaseClient:
    """
    Supabase client for outbound engine data operations.

    Handles all database interactions with proper error handling
    and type safety.
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize Supabase client.

        Args:
            config: Database configuration. If None, loads from environment.
        """
        if config is None:
            config = DatabaseConfig.from_env()

        self.client: Client = create_client(config.url, config.key)

    # ==========================================
    # PROSPECT OPERATIONS
    # ==========================================

    def create_prospect(
        self,
        email: str,
        first_name: str,
        last_name: str,
        company: str,
        title: Optional[str] = None,
        industry: Optional[str] = None,
        company_size: Optional[str] = None,
        linkedin_url: Optional[str] = None,
        custom_fields: Optional[Dict] = None
    ) -> Dict:
        """
        Create a new prospect record.

        Returns:
            Created prospect data with ID
        """
        data = {
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
            "title": title,
            "industry": industry,
            "company_size": company_size,
            "linkedin_url": linkedin_url,
            "custom_fields": custom_fields or {}
        }

        result = self.client.table("prospects").insert(data).execute()
        return result.data[0] if result.data else {}

    def get_prospect_by_email(self, email: str) -> Optional[Dict]:
        """Fetch prospect by email address."""
        result = self.client.table("prospects").select("*").eq("email", email).execute()
        return result.data[0] if result.data else None

    def bulk_create_prospects(self, prospects: List[Dict]) -> List[Dict]:
        """
        Bulk insert prospects with upsert (update if exists).

        Args:
            prospects: List of prospect dictionaries

        Returns:
            List of created/updated prospect records
        """
        result = self.client.table("prospects").upsert(
            prospects,
            on_conflict="email"
        ).execute()
        return result.data

    def list_prospects(
        self,
        limit: int = 100,
        offset: int = 0,
        company: Optional[str] = None
    ) -> List[Dict]:
        """List prospects with optional filtering."""
        query = self.client.table("prospects").select("*")

        if company:
            query = query.eq("company", company)

        result = query.range(offset, offset + limit - 1).execute()
        return result.data

    # ==========================================
    # A/B TEST OPERATIONS
    # ==========================================

    def create_test(
        self,
        test_id: str,
        test_name: str,
        variant_type: str,
        variants: List[str],
        campaign_name: Optional[str] = None
    ) -> Dict:
        """
        Create a new A/B test with variants.

        Args:
            test_id: Unique test identifier
            test_name: Human-readable name
            variant_type: Type of element being tested
            variants: List of variant content strings
            campaign_name: Associated campaign name

        Returns:
            Created test data
        """
        # Create test record
        test_data = {
            "test_id": test_id,
            "test_name": test_name,
            "variant_type": variant_type,
            "campaign_name": campaign_name,
            "status": "running"
        }

        test_result = self.client.table("tests").insert(test_data).execute()

        # Create variant records
        variant_records = [
            {
                "variant_id": f"{test_id}_v{i}",
                "test_id": test_id,
                "content": content
            }
            for i, content in enumerate(variants)
        ]

        self.client.table("variants").insert(variant_records).execute()

        return test_result.data[0] if test_result.data else {}

    def get_test(self, test_id: str) -> Optional[Dict]:
        """Fetch test by ID with variants."""
        test = self.client.table("tests").select("*").eq("test_id", test_id).execute()

        if not test.data:
            return None

        variants = self.client.table("variants").select("*").eq("test_id", test_id).execute()

        result = test.data[0]
        result["variants"] = variants.data
        return result

    def get_variants_for_test(self, test_id: str) -> List[Dict]:
        """Get all variants for a test."""
        result = self.client.table("variants").select("*").eq("test_id", test_id).execute()
        return result.data

    def update_test_status(self, test_id: str, status: str, winner_id: Optional[str] = None):
        """Update test status and optionally set winner."""
        data = {"status": status}
        if winner_id:
            data["winner_id"] = winner_id

        self.client.table("tests").update(data).eq("test_id", test_id).execute()

    # ==========================================
    # SEND TRACKING OPERATIONS
    # ==========================================

    def record_send(
        self,
        send_id: str,
        variant_id: str,
        prospect_email: str,
        campaign_id: Optional[str] = None
    ) -> Dict:
        """
        Record an email send event.

        Also increments the variant's send count.
        """
        send_data = {
            "send_id": send_id,
            "variant_id": variant_id,
            "prospect_email": prospect_email,
            "campaign_id": campaign_id
        }

        result = self.client.table("sends").insert(send_data).execute()

        # Increment variant send count
        self.client.rpc("increment_variant_sends", {"v_id": variant_id}).execute()

        return result.data[0] if result.data else {}

    def record_open(self, send_id: str):
        """Record an email open event."""
        self.client.table("sends").update({
            "opened_at": datetime.utcnow().isoformat()
        }).eq("send_id", send_id).execute()

        # Get variant_id and increment opens
        send = self.client.table("sends").select("variant_id").eq("send_id", send_id).execute()
        if send.data:
            variant_id = send.data[0]["variant_id"]
            self.client.rpc("increment_variant_opens", {"v_id": variant_id}).execute()

    def record_reply(self, send_id: str, sentiment: str = "neutral"):
        """Record a reply event with sentiment."""
        self.client.table("sends").update({
            "replied_at": datetime.utcnow().isoformat(),
            "reply_sentiment": sentiment
        }).eq("send_id", send_id).execute()

        # Get variant_id and increment replies
        send = self.client.table("sends").select("variant_id").eq("send_id", send_id).execute()
        if send.data:
            variant_id = send.data[0]["variant_id"]
            self.client.rpc("increment_variant_replies", {
                "v_id": variant_id,
                "is_positive": sentiment == "positive"
            }).execute()

    def record_bounce(self, send_id: str):
        """Record a bounce event."""
        self.client.table("sends").update({
            "bounced": True
        }).eq("send_id", send_id).execute()

    # ==========================================
    # ANALYTICS QUERIES
    # ==========================================

    def get_variant_performance(self, test_id: str) -> List[Dict]:
        """Get variant performance metrics for a test."""
        result = self.client.table("variant_performance").select("*").eq("test_id", test_id).execute()
        return result.data

    def get_daily_metrics(self, days: int = 14) -> List[Dict]:
        """Get daily send/open/reply metrics."""
        result = self.client.table("daily_metrics").select("*").limit(days).execute()
        return result.data

    def get_test_results(self, test_id: str) -> Dict:
        """
        Get comprehensive test results.

        Returns:
            Dict with test info, variants, and computed metrics
        """
        test = self.get_test(test_id)
        if not test:
            return {}

        variants = self.get_variant_performance(test_id)

        total_sends = sum(v.get("sends", 0) for v in variants)
        total_opens = sum(v.get("opens", 0) for v in variants)
        total_replies = sum(v.get("replies", 0) for v in variants)

        return {
            "test_id": test_id,
            "test_name": test.get("test_name"),
            "status": test.get("status"),
            "winner_id": test.get("winner_id"),
            "variants": variants,
            "summary": {
                "total_sends": total_sends,
                "total_opens": total_opens,
                "total_replies": total_replies,
                "overall_open_rate": round((total_opens / total_sends * 100), 2) if total_sends > 0 else 0,
                "overall_reply_rate": round((total_replies / total_sends * 100), 2) if total_sends > 0 else 0
            }
        }

    # ==========================================
    # CAMPAIGN OPERATIONS
    # ==========================================

    def create_campaign(
        self,
        name: str,
        value_prop: str,
        prospects_count: int = 0,
        instantly_campaign_id: Optional[str] = None
    ) -> Dict:
        """Create a new campaign record."""
        data = {
            "name": name,
            "value_prop": value_prop,
            "prospects_count": prospects_count,
            "instantly_campaign_id": instantly_campaign_id,
            "status": "draft"
        }

        result = self.client.table("campaigns").insert(data).execute()
        return result.data[0] if result.data else {}

    def update_campaign_stats(self, campaign_id: str, stats: Dict):
        """Update campaign aggregate statistics."""
        self.client.table("campaigns").update(stats).eq("id", campaign_id).execute()

    def list_campaigns(self, status: Optional[str] = None) -> List[Dict]:
        """List all campaigns, optionally filtered by status."""
        query = self.client.table("campaigns").select("*").order("created_at", desc=True)

        if status:
            query = query.eq("status", status)

        result = query.execute()
        return result.data


# Singleton instance for convenience
_client: Optional[SupabaseClient] = None


def get_client() -> SupabaseClient:
    """Get or create singleton Supabase client."""
    global _client
    if _client is None:
        _client = SupabaseClient()
    return _client
