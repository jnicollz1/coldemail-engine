# Outbound Engine

A cold email automation framework with LLM-powered copy generation and A/B testing.

Built to solve a specific problem: running structured experiments on outbound copy without manual tracking hell.

## What This Does

1. **Generates email variants using Claude** — subject lines, opening lines, full bodies
2. **Runs A/B tests with statistical tracking** — knows when you have a real winner vs. noise
3. **Integrates with Instantly** for warming, sending, and engagement tracking
4. **Syncs results back** to see which copy actually converts

## How the Workflow Fits Together

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         OUTBOUND WORKFLOW                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. LEAD SOURCING (External)                                            │
│     └── Apollo, Clay, LinkedIn Sales Nav, ZoomInfo, etc.                │
│     └── Export enriched leads as CSV with email, name, company, title   │
│                                                                         │
│  2. LEAD IMPORT → This repo                                             │
│     └── Load prospects into Supabase via Prospect dataclass             │
│     └── Validate emails, dedupe, enrich with custom fields              │
│                                                                         │
│  3. COPY GENERATION → outbound_engine.py                                │
│     └── Claude generates personalized subject lines, openers, bodies    │
│     └── Creates A/B test variants automatically                         │
│                                                                         │
│  4. CAMPAIGN EXECUTION → instantly.py                                   │
│     └── Upload leads + copy to Instantly                                │
│     └── Instantly handles warmup, scheduling, deliverability            │
│                                                                         │
│  5. ENGAGEMENT TRACKING → analytics.py                                  │
│     └── Sync opens/replies from Instantly back to variant stats         │
│     └── Chi-squared significance testing on variants                    │
│     └── Surface winning copy with statistical confidence                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Lead Sourcing

This framework intentionally **does not include lead scraping or enrichment**. Lead data comes from external tools:

| Tool | Use Case |
|------|----------|
| [Apollo.io](https://apollo.io) | B2B contact database + email finder |
| [Clay](https://clay.com) | Waterfall enrichment from 50+ sources |
| [LinkedIn Sales Navigator](https://linkedin.com/sales) | ICP filtering + export |
| [ZoomInfo](https://zoominfo.com) | Enterprise contact data |
| [Clearbit](https://clearbit.com) | Company + contact enrichment |

Export your enriched list as CSV, then load into the system using the lead importer.

### Lead Import

The `leads/` module handles CSV import with validation and deduplication:

```python
from leads import LeadImporter

# Import from any CSV export
importer = LeadImporter()
result = importer.import_csv("leads/example_leads.csv")

print(result.summary())
# Import complete: 10 imported, 0 duplicates skipped, 0 invalid rows

# Access the prospects
for prospect in result.prospects:
    print(f"{prospect.first_name} {prospect.last_name} - {prospect.company}")
```

**Features:**
- Auto-detects column mappings from Apollo, Clay, LinkedIn, ZoomInfo exports
- Validates email format and filters generic addresses (info@, sales@, etc.)
- Deduplicates by email
- Tracks invalid rows with specific error reasons
- CLI support: `python -m leads.importer leads.csv --validate-only`

**Expected CSV format:**
```csv
email,first_name,last_name,company,title,industry,company_size
sarah.chen@acme.io,Sarah,Chen,Acme Corp,VP Sales,SaaS,50-200
```

See `leads/example_leads.csv` for a complete sample.

## Why I Built This

I was running outbound at scale and got tired of:
- Manually tracking which subject lines I'd tested
- Guessing whether 3% vs 4% reply rate was real or random
- Copy/pasting personalization into templates

This automates the tedious parts so I can focus on strategy and iteration.

## Architecture

```
outbound_engine.py      # Core: copy generation, A/B test management, campaign orchestration
instantly.py            # Instantly.ai API client
analytics.py            # Performance tracking, visualization, health monitoring
leads/
├── importer.py         # CSV import with validation and deduplication
└── example_leads.csv   # Sample data format
database/
├── schema.sql          # Supabase/PostgreSQL schema
├── functions.sql       # Database functions for atomic operations
└── supabase_client.py  # Python client for Supabase
```

## Data Security

**Prospect data is stored in a hosted Supabase (PostgreSQL) database**, not locally. This ensures:

- **PII protection** — Customer emails and contact data never sit on local machines
- **SOC 2 compliant infrastructure** — Supabase provides enterprise-grade security
- **Row-level security** — Data isolation between users/organizations
- **Audit trail** — All data operations are logged
- **Encryption at rest** — All data encrypted in the database

The local SQLite fallback (`ab_tests.db`) is available for development/testing only and should never be used with real prospect data.

## Quick Start

```python
from outbound_engine import OutboundCampaign, Prospect

# Initialize
campaign_manager = OutboundCampaign(
    anthropic_api_key="your-claude-key",
    platform_api_key="your-instantly-key"
)

# Define prospects
prospects = [
    Prospect(
        email="vp@target.com",
        first_name="Sarah",
        last_name="Chen",
        company="Acme Corp",
        title="VP Sales",
        industry="SaaS"
    )
]

# Create campaign with A/B tests
campaign = campaign_manager.create_campaign(
    campaign_name="Q1 Enterprise Push",
    prospects=prospects,
    value_prop="Cut ramp time for new AEs by 40%",
    test_subject_lines=True,
    test_opening_lines=True,
    num_variants=3
)

# Generate personalized email for each prospect
for prospect in prospects:
    email = campaign_manager.generate_email_for_prospect(prospect, campaign)
    print(f"Subject: {email['subject']}")
    print(f"Body: {email['body']}")
```

## A/B Testing

The system tracks sends, opens, and replies per variant, then uses chi-squared tests to determine when results are statistically significant.

```python
from outbound_engine import ABTestManager

ab = ABTestManager()

# Check if test has reached significance
result = ab.check_significance("test_20240115_subjects")

print(result)
# {
#   "significant": True,
#   "p_value": 0.023,
#   "winner_id": "test_20240115_subjects_v2",
#   "winner_rate": 0.047,
#   "variant_rates": {...}
# }
```

## Copy Generation

The `CopyGenerator` class uses Claude with prompts tuned for cold outbound:
- Short subject lines (6 words max)
- Personalized openers that show research
- Concise bodies (75 words max)
- Soft vs hard CTAs

```python
from outbound_engine import CopyGenerator, Prospect

generator = CopyGenerator(api_key="your-key")

prospect = Prospect(
    email="john@acme.com",
    first_name="John",
    company="Acme Corp",
    title="VP Sales",
    industry="SaaS",
    company_size="50-200"
)

# Generate 3 subject line variants
subjects = generator.generate_subject_lines(
    prospect,
    value_prop="Reduce sales ramp time by 40%",
    num_variants=3,
    style="casual"
)
# ['acme's ramp time problem', 'quick question re: onboarding', 'saw you're hiring AEs']
```

## Analytics

```python
from analytics import CampaignAnalytics

analytics = CampaignAnalytics()

# Get variant performance
df = analytics.get_variant_performance("test_id")

# Generate full report with recommendations
report = analytics.generate_report("test_id")

# Visualize (returns Altair chart)
chart = analytics.plot_variant_comparison("test_id")
```

## Health Monitoring

```python
from analytics import HealthMonitor

monitor = HealthMonitor()

# Check campaign metrics
alerts = monitor.check_campaign_health({
    "sends": 1000,
    "opens": 180,
    "replies": 12,
    "bounces": 85
})
# [{"level": "critical", "message": "High bounce rate (8.5%) - check list quality"}]
```

## Setup

```bash
pip install -r requirements.txt
```

### Environment Variables

```bash
# Required
export ANTHROPIC_API_KEY="your-claude-key"
export INSTANTLY_API_KEY="your-instantly-key"

# Supabase (for production)
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-supabase-anon-key"
```

### Database Setup

1. Create a [Supabase](https://supabase.com) project (free tier works)
2. Run `database/schema.sql` in the SQL Editor
3. Run `database/functions.sql` for helper functions
4. Copy your project URL and anon key to environment variables

For local development/testing, the system falls back to SQLite (`ab_tests.db`).

## Limitations

- Instantly integration only (Smartlead coming)
- No built-in lead enrichment (use Clay, Apollo, etc.)
- Statistical tests assume independent samples

