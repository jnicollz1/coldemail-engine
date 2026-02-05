-- Supabase Schema for Outbound Engine
-- Run this in your Supabase SQL Editor to set up the database

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- PROSPECTS TABLE
-- Stores prospect/lead data securely in hosted DB
-- ============================================
CREATE TABLE IF NOT EXISTS prospects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    company TEXT NOT NULL,
    title TEXT,
    industry TEXT,
    company_size TEXT,
    linkedin_url TEXT,
    custom_fields JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for email lookups
CREATE INDEX IF NOT EXISTS idx_prospects_email ON prospects(email);
CREATE INDEX IF NOT EXISTS idx_prospects_company ON prospects(company);

-- ============================================
-- A/B TESTS TABLE
-- Tracks test configurations and status
-- ============================================
CREATE TABLE IF NOT EXISTS tests (
    test_id TEXT PRIMARY KEY,
    test_name TEXT NOT NULL,
    variant_type TEXT NOT NULL CHECK (variant_type IN ('subject_line', 'opening_line', 'cta', 'full_body')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT DEFAULT 'running' CHECK (status IN ('running', 'completed', 'paused')),
    winner_id TEXT,
    campaign_name TEXT
);

CREATE INDEX IF NOT EXISTS idx_tests_status ON tests(status);
CREATE INDEX IF NOT EXISTS idx_tests_campaign ON tests(campaign_name);

-- ============================================
-- VARIANTS TABLE
-- Individual test variants with performance metrics
-- ============================================
CREATE TABLE IF NOT EXISTS variants (
    variant_id TEXT PRIMARY KEY,
    test_id TEXT NOT NULL REFERENCES tests(test_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    sends INTEGER DEFAULT 0,
    opens INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    positive_replies INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_variants_test_id ON variants(test_id);

-- ============================================
-- SENDS TABLE
-- Individual email send records with engagement tracking
-- ============================================
CREATE TABLE IF NOT EXISTS sends (
    send_id TEXT PRIMARY KEY,
    variant_id TEXT NOT NULL REFERENCES variants(variant_id) ON DELETE CASCADE,
    prospect_id UUID REFERENCES prospects(id) ON DELETE SET NULL,
    prospect_email TEXT NOT NULL,
    sent_at TIMESTAMPTZ DEFAULT NOW(),
    opened_at TIMESTAMPTZ,
    replied_at TIMESTAMPTZ,
    reply_sentiment TEXT CHECK (reply_sentiment IN ('positive', 'neutral', 'negative')),
    bounced BOOLEAN DEFAULT FALSE,
    campaign_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_sends_variant_id ON sends(variant_id);
CREATE INDEX IF NOT EXISTS idx_sends_prospect_email ON sends(prospect_email);
CREATE INDEX IF NOT EXISTS idx_sends_sent_at ON sends(sent_at);

-- ============================================
-- CAMPAIGNS TABLE
-- High-level campaign metadata
-- ============================================
CREATE TABLE IF NOT EXISTS campaigns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    value_prop TEXT,
    status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'paused', 'completed')),
    instantly_campaign_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    prospects_count INTEGER DEFAULT 0,
    total_sends INTEGER DEFAULT 0,
    total_opens INTEGER DEFAULT 0,
    total_replies INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status);

-- ============================================
-- ROW LEVEL SECURITY (RLS)
-- Ensures data isolation per user/organization
-- ============================================

-- Enable RLS on all tables
ALTER TABLE prospects ENABLE ROW LEVEL SECURITY;
ALTER TABLE tests ENABLE ROW LEVEL SECURITY;
ALTER TABLE variants ENABLE ROW LEVEL SECURITY;
ALTER TABLE sends ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns ENABLE ROW LEVEL SECURITY;

-- Policies (adjust based on your auth setup)
-- These allow authenticated users to access their own data

CREATE POLICY "Users can view own prospects" ON prospects
    FOR SELECT USING (auth.role() = 'authenticated');

CREATE POLICY "Users can insert own prospects" ON prospects
    FOR INSERT WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY "Users can update own prospects" ON prospects
    FOR UPDATE USING (auth.role() = 'authenticated');

CREATE POLICY "Users can view own tests" ON tests
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Users can view own variants" ON variants
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Users can view own sends" ON sends
    FOR ALL USING (auth.role() = 'authenticated');

CREATE POLICY "Users can view own campaigns" ON campaigns
    FOR ALL USING (auth.role() = 'authenticated');

-- ============================================
-- USEFUL VIEWS
-- ============================================

-- Variant performance summary
CREATE OR REPLACE VIEW variant_performance AS
SELECT
    v.variant_id,
    v.test_id,
    t.test_name,
    v.content,
    v.sends,
    v.opens,
    v.replies,
    v.positive_replies,
    CASE WHEN v.sends > 0 THEN ROUND((v.opens::NUMERIC / v.sends) * 100, 2) ELSE 0 END as open_rate,
    CASE WHEN v.sends > 0 THEN ROUND((v.replies::NUMERIC / v.sends) * 100, 2) ELSE 0 END as reply_rate,
    CASE WHEN v.replies > 0 THEN ROUND((v.positive_replies::NUMERIC / v.replies) * 100, 2) ELSE 0 END as positive_rate
FROM variants v
JOIN tests t ON v.test_id = t.test_id;

-- Daily send metrics
CREATE OR REPLACE VIEW daily_metrics AS
SELECT
    DATE(sent_at) as date,
    COUNT(*) as sends,
    COUNT(opened_at) as opens,
    COUNT(replied_at) as replies,
    COUNT(*) FILTER (WHERE bounced = TRUE) as bounces
FROM sends
GROUP BY DATE(sent_at)
ORDER BY date DESC;

-- ============================================
-- FUNCTIONS
-- ============================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to tables with updated_at
CREATE TRIGGER update_prospects_updated_at
    BEFORE UPDATE ON prospects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_campaigns_updated_at
    BEFORE UPDATE ON campaigns
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
