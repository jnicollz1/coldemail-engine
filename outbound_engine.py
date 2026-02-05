"""
Outbound Engine - Cold Email Automation with LLM Copy Generation

A lightweight wrapper around Instantly/Smartlead that adds:
- LLM-powered email personalization
- Structured A/B testing with statistical significance
- Performance tracking and variant promotion

Author: Jake Nicoll
"""

import anthropic
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from dataclasses import dataclass
from enum import Enum
import json
import sqlite3
import random
from scipy import stats


class VariantType(Enum):
    """Types of email elements we can test."""
    SUBJECT_LINE = "subject_line"
    OPENING_LINE = "opening_line"
    CTA = "cta"
    FULL_BODY = "full_body"


@dataclass
class Prospect:
    """Represents a single prospect for outreach."""
    email: str
    first_name: str
    last_name: str
    company: str
    title: str
    industry: Optional[str] = None
    company_size: Optional[str] = None
    linkedin_url: Optional[str] = None
    custom_fields: Optional[Dict] = None
    
    def to_context_string(self) -> str:
        """Format prospect data for LLM context."""
        context = f"""
        Name: {self.first_name} {self.last_name}
        Title: {self.title}
        Company: {self.company}
        Industry: {self.industry or 'Unknown'}
        Company Size: {self.company_size or 'Unknown'}
        """
        if self.custom_fields:
            for key, value in self.custom_fields.items():
                context += f"{key}: {value}\n"
        return context.strip()


@dataclass
class EmailVariant:
    """A single variant in an A/B test."""
    variant_id: str
    variant_type: VariantType
    content: str
    sends: int = 0
    opens: int = 0
    replies: int = 0
    positive_replies: int = 0
    
    @property
    def open_rate(self) -> float:
        return self.opens / self.sends if self.sends > 0 else 0
    
    @property
    def reply_rate(self) -> float:
        return self.replies / self.sends if self.sends > 0 else 0


class CopyGenerator:
    """
    Generates personalized email copy using Claude.
    
    Design philosophy:
    - Emails should sound human, not templated
    - Personalization should be specific, not just {first_name}
    - Short > long for cold outreach
    """
    
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-20250514"
    
    def generate_subject_lines(
        self, 
        prospect: Prospect,
        value_prop: str,
        num_variants: int = 3,
        style: str = "casual"
    ) -> List[str]:
        """
        Generate subject line variants for A/B testing.
        
        Args:
            prospect: Target prospect data
            value_prop: Core value proposition to communicate
            num_variants: Number of variants to generate
            style: Tone - 'casual', 'professional', or 'provocative'
        
        Returns:
            List of subject line strings
        """
        prompt = f"""Generate {num_variants} cold email subject lines for this prospect.

PROSPECT:
{prospect.to_context_string()}

VALUE PROP: {value_prop}

STYLE: {style}

RULES:
- Max 6 words (short subject lines win)
- No spam trigger words (free, guarantee, act now)
- No ALL CAPS
- Lowercase can work well
- Questions can work but don't overuse
- Reference something specific when possible (company, role, industry)

Return ONLY the subject lines, one per line, no numbering or explanation."""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        
        lines = response.content[0].text.strip().split('\n')
        return [line.strip() for line in lines if line.strip()][:num_variants]
    
    def generate_opening_lines(
        self,
        prospect: Prospect,
        num_variants: int = 3
    ) -> List[str]:
        """
        Generate personalized opening lines.
        
        The opening line is the most important part of a cold email.
        It needs to show you did research, not just mail-merged.
        """
        prompt = f"""Generate {num_variants} opening lines for a cold email to this prospect.

PROSPECT:
{prospect.to_context_string()}

RULES:
- Reference something specific about them or their company
- No "I hope this finds you well" or similar
- No "My name is..." openers
- Should feel like you actually looked them up
- One sentence max
- Don't be creepy (avoid referencing personal social media)

GOOD EXAMPLES:
- "Saw {self.company} just expanded into APAC - congrats on the growth."
- "Your post on rethinking sales comp was spot on."
- "Noticed you're hiring 3 AEs - guessing pipeline gen is top of mind."

Return ONLY the opening lines, one per line."""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        
        lines = response.content[0].text.strip().split('\n')
        return [line.strip() for line in lines if line.strip()][:num_variants]
    
    def generate_full_email(
        self,
        prospect: Prospect,
        value_prop: str,
        subject_line: str,
        opening_line: str,
        cta_style: str = "soft"
    ) -> str:
        """
        Generate complete email body.
        
        Args:
            prospect: Target prospect
            value_prop: What you're offering
            subject_line: Already-selected subject
            opening_line: Already-selected opener
            cta_style: 'soft' (interest-based) or 'hard' (meeting request)
        
        Returns:
            Complete email body string
        """
        cta_guidance = {
            "soft": "End with a low-commitment ask like 'worth exploring?' or 'make sense to chat?'",
            "hard": "End with a specific meeting request like 'Do you have 15 min Tuesday or Wednesday?'"
        }
        
        prompt = f"""Write a cold email body for this prospect.

PROSPECT:
{prospect.to_context_string()}

VALUE PROP: {value_prop}
SUBJECT LINE: {subject_line}
OPENING LINE: {opening_line}

CTA STYLE: {cta_guidance.get(cta_style, cta_guidance['soft'])}

RULES:
- Start with the opening line provided
- Max 75 words total (shorter is better)
- One clear value prop, not a feature list
- No "I" as the first word
- No attachments or "see below"
- Sound like a human, not a sales bot
- End with the CTA, nothing after

Return ONLY the email body."""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text.strip()


class ABTestManager:
    """
    Manages A/B tests with statistical significance tracking.
    
    Uses chi-squared test to determine when a variant 
    has won with 95% confidence.
    """
    
    def __init__(self, db_path: str = "ab_tests.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database for test tracking."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tests (
                test_id TEXT PRIMARY KEY,
                test_name TEXT,
                variant_type TEXT,
                created_at TIMESTAMP,
                status TEXT DEFAULT 'running',
                winner_id TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS variants (
                variant_id TEXT PRIMARY KEY,
                test_id TEXT,
                content TEXT,
                sends INTEGER DEFAULT 0,
                opens INTEGER DEFAULT 0,
                replies INTEGER DEFAULT 0,
                positive_replies INTEGER DEFAULT 0,
                FOREIGN KEY (test_id) REFERENCES tests(test_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sends (
                send_id TEXT PRIMARY KEY,
                variant_id TEXT,
                prospect_email TEXT,
                sent_at TIMESTAMP,
                opened_at TIMESTAMP,
                replied_at TIMESTAMP,
                reply_sentiment TEXT,
                FOREIGN KEY (variant_id) REFERENCES variants(variant_id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def create_test(
        self,
        test_name: str,
        variant_type: VariantType,
        variants: List[str]
    ) -> str:
        """
        Create a new A/B test with variants.
        
        Args:
            test_name: Human-readable test name
            variant_type: What element we're testing
            variants: List of variant content strings
        
        Returns:
            test_id string
        """
        test_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO tests (test_id, test_name, variant_type, created_at)
            VALUES (?, ?, ?, ?)
        """, (test_id, test_name, variant_type.value, datetime.now()))
        
        for i, content in enumerate(variants):
            variant_id = f"{test_id}_v{i}"
            cursor.execute("""
                INSERT INTO variants (variant_id, test_id, content)
                VALUES (?, ?, ?)
            """, (variant_id, test_id, content))
        
        conn.commit()
        conn.close()
        
        return test_id
    
    def get_variant_for_send(self, test_id: str) -> tuple[str, str]:
        """
        Get a variant for sending (random assignment).
        
        Returns:
            Tuple of (variant_id, content)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT variant_id, content FROM variants
            WHERE test_id = ?
        """, (test_id,))
        
        variants = cursor.fetchall()
        conn.close()
        
        if not variants:
            raise ValueError(f"No variants found for test {test_id}")
        
        # Random assignment
        chosen = random.choice(variants)
        return chosen[0], chosen[1]
    
    def record_send(self, variant_id: str, prospect_email: str) -> str:
        """Record that an email was sent."""
        send_id = f"send_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{random.randint(1000,9999)}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO sends (send_id, variant_id, prospect_email, sent_at)
            VALUES (?, ?, ?, ?)
        """, (send_id, variant_id, prospect_email, datetime.now()))
        
        cursor.execute("""
            UPDATE variants SET sends = sends + 1 WHERE variant_id = ?
        """, (variant_id,))
        
        conn.commit()
        conn.close()
        
        return send_id
    
    def record_open(self, send_id: str):
        """Record that an email was opened."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE sends SET opened_at = ? WHERE send_id = ?
        """, (datetime.now(), send_id))
        
        cursor.execute("""
            UPDATE variants SET opens = opens + 1 
            WHERE variant_id = (SELECT variant_id FROM sends WHERE send_id = ?)
        """, (send_id,))
        
        conn.commit()
        conn.close()
    
    def record_reply(self, send_id: str, sentiment: str = "neutral"):
        """
        Record a reply.
        
        Args:
            send_id: The send to update
            sentiment: 'positive', 'negative', or 'neutral'
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE sends SET replied_at = ?, reply_sentiment = ? WHERE send_id = ?
        """, (datetime.now(), sentiment, send_id))
        
        cursor.execute("""
            UPDATE variants SET replies = replies + 1 
            WHERE variant_id = (SELECT variant_id FROM sends WHERE send_id = ?)
        """, (send_id,))
        
        if sentiment == "positive":
            cursor.execute("""
                UPDATE variants SET positive_replies = positive_replies + 1 
                WHERE variant_id = (SELECT variant_id FROM sends WHERE send_id = ?)
            """, (send_id,))
        
        conn.commit()
        conn.close()
    
    def check_significance(self, test_id: str, metric: str = "replies") -> Dict:
        """
        Check if test has reached statistical significance.
        
        Uses chi-squared test comparing variant performance.
        
        Args:
            test_id: Test to analyze
            metric: 'opens' or 'replies'
        
        Returns:
            Dict with significance status and stats
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT variant_id, sends, opens, replies FROM variants
            WHERE test_id = ?
        """, (test_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if len(rows) < 2:
            return {"significant": False, "reason": "Need at least 2 variants"}
        
        # Build contingency table
        if metric == "opens":
            successes = [row[2] for row in rows]
        else:
            successes = [row[3] for row in rows]
        
        sends = [row[1] for row in rows]
        failures = [s - succ for s, succ in zip(sends, successes)]
        
        # Need minimum sample size
        if min(sends) < 50:
            return {
                "significant": False, 
                "reason": f"Need 50+ sends per variant (min: {min(sends)})"
            }
        
        # Chi-squared test
        contingency = [successes, failures]
        chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
        
        significant = p_value < 0.05
        
        # Find winner
        rates = [succ / send if send > 0 else 0 for succ, send in zip(successes, sends)]
        winner_idx = rates.index(max(rates))
        winner_id = rows[winner_idx][0]
        
        return {
            "significant": significant,
            "p_value": p_value,
            "winner_id": winner_id if significant else None,
            "winner_rate": rates[winner_idx],
            "variant_rates": {rows[i][0]: rates[i] for i in range(len(rows))}
        }
    
    def get_test_results(self, test_id: str) -> pd.DataFrame:
        """Get current results for a test as DataFrame."""
        conn = sqlite3.connect(self.db_path)
        
        df = pd.read_sql_query("""
            SELECT 
                variant_id,
                content,
                sends,
                opens,
                replies,
                positive_replies,
                ROUND(opens * 100.0 / NULLIF(sends, 0), 1) as open_rate,
                ROUND(replies * 100.0 / NULLIF(sends, 0), 1) as reply_rate
            FROM variants
            WHERE test_id = ?
        """, conn, params=(test_id,))
        
        conn.close()
        return df


class OutboundCampaign:
    """
    Orchestrates a complete outbound campaign.
    
    Connects copy generation, A/B testing, and sending platform.
    """
    
    def __init__(
        self,
        anthropic_api_key: str,
        sending_platform: str = "instantly",
        platform_api_key: Optional[str] = None
    ):
        self.copy_generator = CopyGenerator(anthropic_api_key)
        self.ab_manager = ABTestManager()
        self.sending_platform = sending_platform
        self.platform_api_key = platform_api_key
    
    def create_campaign(
        self,
        campaign_name: str,
        prospects: List[Prospect],
        value_prop: str,
        test_subject_lines: bool = True,
        test_opening_lines: bool = True,
        num_variants: int = 3
    ) -> Dict:
        """
        Set up a new campaign with A/B tests.
        
        Args:
            campaign_name: Name for this campaign
            prospects: List of prospects to target
            value_prop: Core value proposition
            test_subject_lines: Whether to A/B test subjects
            test_opening_lines: Whether to A/B test openers
            num_variants: Variants per test
        
        Returns:
            Campaign configuration dict
        """
        campaign = {
            "name": campaign_name,
            "created_at": datetime.now().isoformat(),
            "prospects_count": len(prospects),
            "value_prop": value_prop,
            "tests": {}
        }
        
        # Use first prospect as template for variant generation
        sample_prospect = prospects[0]
        
        if test_subject_lines:
            subjects = self.copy_generator.generate_subject_lines(
                sample_prospect, 
                value_prop, 
                num_variants
            )
            test_id = self.ab_manager.create_test(
                f"{campaign_name}_subjects",
                VariantType.SUBJECT_LINE,
                subjects
            )
            campaign["tests"]["subject_line"] = {
                "test_id": test_id,
                "variants": subjects
            }
        
        if test_opening_lines:
            openers = self.copy_generator.generate_opening_lines(
                sample_prospect,
                num_variants
            )
            test_id = self.ab_manager.create_test(
                f"{campaign_name}_openers",
                VariantType.OPENING_LINE,
                openers
            )
            campaign["tests"]["opening_line"] = {
                "test_id": test_id,
                "variants": openers
            }
        
        return campaign
    
    def generate_email_for_prospect(
        self,
        prospect: Prospect,
        campaign: Dict,
        cta_style: str = "soft"
    ) -> Dict:
        """
        Generate a personalized email for a specific prospect.
        
        Selects variants from active A/B tests and generates full email.
        
        Returns:
            Dict with subject, body, and variant assignments
        """
        result = {"prospect_email": prospect.email, "variants_used": {}}
        
        # Get subject line variant
        if "subject_line" in campaign.get("tests", {}):
            test_id = campaign["tests"]["subject_line"]["test_id"]
            variant_id, subject = self.ab_manager.get_variant_for_send(test_id)
            result["subject"] = subject
            result["variants_used"]["subject_line"] = variant_id
        else:
            # Generate one-off if not testing
            subjects = self.copy_generator.generate_subject_lines(
                prospect, 
                campaign["value_prop"], 
                1
            )
            result["subject"] = subjects[0]
        
        # Get opening line variant
        if "opening_line" in campaign.get("tests", {}):
            test_id = campaign["tests"]["opening_line"]["test_id"]
            variant_id, opener = self.ab_manager.get_variant_for_send(test_id)
            result["opening_line"] = opener
            result["variants_used"]["opening_line"] = variant_id
        else:
            openers = self.copy_generator.generate_opening_lines(prospect, 1)
            result["opening_line"] = openers[0]
        
        # Generate full email body
        result["body"] = self.copy_generator.generate_full_email(
            prospect,
            campaign["value_prop"],
            result["subject"],
            result["opening_line"],
            cta_style
        )
        
        return result
    
    def get_campaign_results(self, campaign: Dict) -> Dict:
        """Get current A/B test results for a campaign."""
        results = {}
        
        for test_type, test_info in campaign.get("tests", {}).items():
            test_id = test_info["test_id"]
            results[test_type] = {
                "data": self.ab_manager.get_test_results(test_id).to_dict(),
                "significance": self.ab_manager.check_significance(test_id)
            }
        
        return results


# Example usage
if __name__ == "__main__":
    # This would use your actual API key
    # campaign_manager = OutboundCampaign(anthropic_api_key="your-key")
    
    # Example prospect
    prospect = Prospect(
        email="john@acme.com",
        first_name="John",
        last_name="Smith",
        company="Acme Corp",
        title="VP Sales",
        industry="SaaS",
        company_size="50-200"
    )
    
    print("Outbound Engine initialized.")
    print(f"Sample prospect: {prospect.first_name} {prospect.last_name} at {prospect.company}")
