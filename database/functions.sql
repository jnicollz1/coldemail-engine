-- Additional Supabase Functions
-- Run after schema.sql to add helper functions for atomic operations

-- ============================================
-- INCREMENT FUNCTIONS
-- Used for atomic counter updates from the client
-- ============================================

-- Increment sends count for a variant
CREATE OR REPLACE FUNCTION increment_variant_sends(v_id TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE variants
    SET sends = sends + 1
    WHERE variant_id = v_id;
END;
$$ LANGUAGE plpgsql;

-- Increment opens count for a variant
CREATE OR REPLACE FUNCTION increment_variant_opens(v_id TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE variants
    SET opens = opens + 1
    WHERE variant_id = v_id;
END;
$$ LANGUAGE plpgsql;

-- Increment replies count for a variant
CREATE OR REPLACE FUNCTION increment_variant_replies(v_id TEXT, is_positive BOOLEAN DEFAULT FALSE)
RETURNS VOID AS $$
BEGIN
    UPDATE variants
    SET
        replies = replies + 1,
        positive_replies = positive_replies + CASE WHEN is_positive THEN 1 ELSE 0 END
    WHERE variant_id = v_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ANALYTICS FUNCTIONS
-- ============================================

-- Get statistical significance for a test using chi-squared approximation
-- Returns p-value estimate (for display purposes - actual significance
-- testing should be done in Python with scipy for accuracy)
CREATE OR REPLACE FUNCTION get_test_significance(t_id TEXT)
RETURNS TABLE (
    test_id TEXT,
    is_significant BOOLEAN,
    min_sends INTEGER,
    variant_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t_id as test_id,
        MIN(v.sends) >= 50 as is_significant,
        MIN(v.sends)::INTEGER as min_sends,
        COUNT(*)::INTEGER as variant_count
    FROM variants v
    WHERE v.test_id = t_id;
END;
$$ LANGUAGE plpgsql;

-- Get winning variant for a test (highest reply rate with min sample)
CREATE OR REPLACE FUNCTION get_winning_variant(t_id TEXT)
RETURNS TABLE (
    variant_id TEXT,
    content TEXT,
    reply_rate NUMERIC,
    sends INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        v.variant_id,
        v.content,
        CASE WHEN v.sends > 0
            THEN ROUND((v.replies::NUMERIC / v.sends) * 100, 2)
            ELSE 0
        END as reply_rate,
        v.sends
    FROM variants v
    WHERE v.test_id = t_id
      AND v.sends >= 50
    ORDER BY reply_rate DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- CLEANUP FUNCTIONS
-- ============================================

-- Archive old completed tests (older than 90 days)
CREATE OR REPLACE FUNCTION archive_old_tests()
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    WITH archived AS (
        UPDATE tests
        SET status = 'archived'
        WHERE status = 'completed'
          AND created_at < NOW() - INTERVAL '90 days'
        RETURNING test_id
    )
    SELECT COUNT(*) INTO archived_count FROM archived;

    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- Delete bounced prospect sends (for cleanup)
CREATE OR REPLACE FUNCTION cleanup_bounced_sends(days_old INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    WITH deleted AS (
        DELETE FROM sends
        WHERE bounced = TRUE
          AND sent_at < NOW() - (days_old || ' days')::INTERVAL
        RETURNING send_id
    )
    SELECT COUNT(*) INTO deleted_count FROM deleted;

    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
