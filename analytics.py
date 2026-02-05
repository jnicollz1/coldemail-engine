"""
Campaign Analytics & Reporting

Provides:
- Performance dashboards
- Variant comparison charts
- Statistical significance visualization
- Campaign health monitoring
"""

import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sqlite3


class CampaignAnalytics:
    """
    Analytics and visualization for outbound campaigns.
    """
    
    def __init__(self, db_path: str = "ab_tests.db"):
        self.db_path = db_path
    
    def get_variant_performance(self, test_id: str) -> pd.DataFrame:
        """
        Get performance metrics for all variants in a test.
        
        Returns DataFrame with rates and confidence intervals.
        """
        conn = sqlite3.connect(self.db_path)
        
        df = pd.read_sql_query("""
            SELECT 
                variant_id,
                SUBSTR(content, 1, 50) as content_preview,
                sends,
                opens,
                replies,
                positive_replies,
                ROUND(opens * 100.0 / NULLIF(sends, 0), 2) as open_rate,
                ROUND(replies * 100.0 / NULLIF(sends, 0), 2) as reply_rate,
                ROUND(positive_replies * 100.0 / NULLIF(replies, 0), 2) as positive_rate
            FROM variants
            WHERE test_id = ?
            ORDER BY reply_rate DESC
        """, conn, params=(test_id,))
        
        conn.close()
        return df
    
    def get_daily_performance(
        self, 
        test_id: str,
        days: int = 14
    ) -> pd.DataFrame:
        """
        Get daily send/open/reply counts for trend analysis.
        """
        conn = sqlite3.connect(self.db_path)
        
        df = pd.read_sql_query("""
            SELECT 
                DATE(sent_at) as date,
                v.variant_id,
                COUNT(*) as sends,
                SUM(CASE WHEN opened_at IS NOT NULL THEN 1 ELSE 0 END) as opens,
                SUM(CASE WHEN replied_at IS NOT NULL THEN 1 ELSE 0 END) as replies
            FROM sends s
            JOIN variants v ON s.variant_id = v.variant_id
            WHERE v.test_id = ?
              AND s.sent_at >= DATE('now', ?)
            GROUP BY DATE(sent_at), v.variant_id
            ORDER BY date
        """, conn, params=(test_id, f'-{days} days'))
        
        conn.close()
        return df
    
    def plot_variant_comparison(self, test_id: str) -> alt.Chart:
        """
        Create bar chart comparing variant performance.
        """
        df = self.get_variant_performance(test_id)
        
        # Reshape for grouped bar chart
        df_long = df.melt(
            id_vars=['variant_id', 'content_preview'],
            value_vars=['open_rate', 'reply_rate'],
            var_name='metric',
            value_name='rate'
        )
        
        chart = alt.Chart(df_long).mark_bar().encode(
            x=alt.X('variant_id:N', title='Variant'),
            y=alt.Y('rate:Q', title='Rate (%)'),
            color=alt.Color('metric:N', title='Metric'),
            xOffset='metric:N'
        ).properties(
            title='Variant Performance Comparison',
            width=400,
            height=300
        )
        
        return chart
    
    def plot_performance_over_time(self, test_id: str) -> alt.Chart:
        """
        Create line chart showing performance trends.
        """
        df = self.get_daily_performance(test_id)
        
        if df.empty:
            return None
        
        # Calculate daily rates
        df['open_rate'] = (df['opens'] / df['sends'] * 100).round(1)
        df['reply_rate'] = (df['replies'] / df['sends'] * 100).round(1)
        
        df_long = df.melt(
            id_vars=['date', 'variant_id'],
            value_vars=['open_rate', 'reply_rate'],
            var_name='metric',
            value_name='rate'
        )
        
        chart = alt.Chart(df_long).mark_line(point=True).encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('rate:Q', title='Rate (%)'),
            color=alt.Color('variant_id:N', title='Variant'),
            strokeDash=alt.StrokeDash('metric:N', title='Metric')
        ).properties(
            title='Performance Over Time',
            width=600,
            height=300
        )
        
        return chart
    
    def plot_significance_progress(self, test_id: str) -> alt.Chart:
        """
        Visualize progress toward statistical significance.
        
        Shows sample size vs required for 95% confidence.
        """
        df = self.get_variant_performance(test_id)
        
        # Minimum sample size rule of thumb: ~400 per variant for 
        # detecting 20% relative improvement at 95% confidence
        min_sample = 400
        
        df['sample_progress'] = (df['sends'] / min_sample * 100).clip(upper=100)
        
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('variant_id:N', title='Variant'),
            y=alt.Y('sample_progress:Q', title='Progress to Significance (%)'),
            color=alt.condition(
                alt.datum.sample_progress >= 100,
                alt.value('#22c55e'),  # Green if complete
                alt.value('#3b82f6')   # Blue if in progress
            )
        ).properties(
            title=f'Sample Size Progress (target: {min_sample} per variant)',
            width=300,
            height=200
        )
        
        # Add reference line at 100%
        rule = alt.Chart(pd.DataFrame({'y': [100]})).mark_rule(
            color='red',
            strokeDash=[5, 5]
        ).encode(y='y:Q')
        
        return chart + rule
    
    def generate_report(self, test_id: str) -> Dict:
        """
        Generate a complete test report.
        
        Returns dict with all metrics and recommendations.
        """
        df = self.get_variant_performance(test_id)
        
        if df.empty:
            return {"error": "No data found for test"}
        
        # Find leader
        leader = df.iloc[0]  # Already sorted by reply_rate DESC
        
        # Calculate lift vs average
        avg_reply_rate = df['reply_rate'].mean()
        leader_lift = ((leader['reply_rate'] - avg_reply_rate) / avg_reply_rate * 100) if avg_reply_rate > 0 else 0
        
        report = {
            "test_id": test_id,
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_sends": int(df['sends'].sum()),
                "total_opens": int(df['opens'].sum()),
                "total_replies": int(df['replies'].sum()),
                "overall_open_rate": round(df['opens'].sum() / df['sends'].sum() * 100, 2) if df['sends'].sum() > 0 else 0,
                "overall_reply_rate": round(df['replies'].sum() / df['sends'].sum() * 100, 2) if df['sends'].sum() > 0 else 0
            },
            "leader": {
                "variant_id": leader['variant_id'],
                "content": leader['content_preview'],
                "reply_rate": leader['reply_rate'],
                "lift_vs_average": round(leader_lift, 1)
            },
            "variants": df.to_dict(orient='records'),
            "recommendation": self._generate_recommendation(df)
        }
        
        return report
    
    def _generate_recommendation(self, df: pd.DataFrame) -> str:
        """Generate actionable recommendation based on results."""
        total_sends = df['sends'].sum()
        
        if total_sends < 200:
            return "Continue testing - need more volume for reliable results (target: 400+ sends per variant)"
        
        # Check if there's a clear winner
        rates = df['reply_rate'].tolist()
        if len(rates) >= 2:
            best = max(rates)
            second = sorted(rates, reverse=True)[1]
            
            if best > 0 and (best - second) / best > 0.2:  # 20%+ relative difference
                return f"Strong signal: leading variant outperforms by {round((best-second)/second*100)}%. Consider promoting to 100% of traffic."
            else:
                return "Results are close - continue testing or consider if variants are meaningfully different"
        
        return "Insufficient data for recommendation"


class HealthMonitor:
    """
    Monitors campaign and account health.
    
    Tracks deliverability signals and warns of issues.
    """
    
    def __init__(self, db_path: str = "ab_tests.db"):
        self.db_path = db_path
        
        # Alert thresholds
        self.thresholds = {
            "bounce_rate": 5.0,      # Alert if >5% bounces
            "spam_rate": 0.1,        # Alert if >0.1% spam reports
            "open_rate_low": 15.0,   # Alert if <15% opens (deliverability issue)
            "reply_rate_low": 1.0    # Alert if <1% replies (content issue)
        }
    
    def check_campaign_health(self, campaign_stats: Dict) -> List[Dict]:
        """
        Check campaign metrics against thresholds.
        
        Args:
            campaign_stats: Dict with sends, bounces, opens, replies, spam_reports
        
        Returns:
            List of alert dicts
        """
        alerts = []
        
        sends = campaign_stats.get('sends', 0)
        if sends == 0:
            return [{"level": "info", "message": "No sends yet"}]
        
        # Bounce rate check
        bounce_rate = campaign_stats.get('bounces', 0) / sends * 100
        if bounce_rate > self.thresholds['bounce_rate']:
            alerts.append({
                "level": "critical",
                "metric": "bounce_rate",
                "value": round(bounce_rate, 2),
                "threshold": self.thresholds['bounce_rate'],
                "message": f"High bounce rate ({bounce_rate:.1f}%) - check list quality"
            })
        
        # Open rate check
        open_rate = campaign_stats.get('opens', 0) / sends * 100
        if open_rate < self.thresholds['open_rate_low'] and sends > 100:
            alerts.append({
                "level": "warning",
                "metric": "open_rate",
                "value": round(open_rate, 2),
                "threshold": self.thresholds['open_rate_low'],
                "message": f"Low open rate ({open_rate:.1f}%) - possible deliverability issue"
            })
        
        # Reply rate check
        reply_rate = campaign_stats.get('replies', 0) / sends * 100
        if reply_rate < self.thresholds['reply_rate_low'] and sends > 200:
            alerts.append({
                "level": "warning",
                "metric": "reply_rate", 
                "value": round(reply_rate, 2),
                "threshold": self.thresholds['reply_rate_low'],
                "message": f"Low reply rate ({reply_rate:.1f}%) - review copy/targeting"
            })
        
        if not alerts:
            alerts.append({
                "level": "ok",
                "message": "All metrics within healthy ranges"
            })
        
        return alerts
    
    def check_sending_account_health(self, account_stats: Dict) -> List[Dict]:
        """
        Check sending account health.
        
        Args:
            account_stats: Dict with daily_limit, sent_today, warmup_day, reputation
        
        Returns:
            List of alert dicts
        """
        alerts = []
        
        # Check if near daily limit
        daily_limit = account_stats.get('daily_limit', 50)
        sent_today = account_stats.get('sent_today', 0)
        
        if sent_today >= daily_limit * 0.9:
            alerts.append({
                "level": "warning",
                "message": f"Near daily limit ({sent_today}/{daily_limit})"
            })
        
        # Check warmup progress
        warmup_day = account_stats.get('warmup_day', 0)
        if warmup_day < 14:
            alerts.append({
                "level": "info",
                "message": f"Account still warming (day {warmup_day}/14 minimum)"
            })
        
        # Check reputation
        reputation = account_stats.get('reputation', 100)
        if reputation < 80:
            alerts.append({
                "level": "critical",
                "message": f"Low sender reputation ({reputation}%) - pause and investigate"
            })
        elif reputation < 95:
            alerts.append({
                "level": "warning",
                "message": f"Sender reputation declining ({reputation}%)"
            })
        
        if not alerts:
            alerts.append({
                "level": "ok",
                "message": "Account healthy"
            })
        
        return alerts


# Example usage
if __name__ == "__main__":
    analytics = CampaignAnalytics()
    monitor = HealthMonitor()
    
    print("Analytics module loaded.")
    print("Use CampaignAnalytics for performance reports and charts.")
    print("Use HealthMonitor to check campaign/account health.")
