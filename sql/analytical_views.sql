-- ============================================================
-- Analytical VIEWs consumed by the dashboard
-- ============================================================

DROP VIEW IF EXISTS v_spam_overview;
CREATE VIEW v_spam_overview AS
SELECT
    COUNT(*)                                               AS total_emails,
    SUM(CASE WHEN is_spam = 1 THEN 1 ELSE 0 END)           AS spam_count,
    SUM(CASE WHEN is_spam = 0 THEN 1 ELSE 0 END)           AS ham_count,
    SUM(CASE WHEN is_spam IS NULL THEN 1 ELSE 0 END)       AS unlabeled_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_spam = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN is_spam IS NOT NULL THEN 1 ELSE 0 END), 0),
        2
    )                                                      AS spam_rate_pct,
    ROUND(AVG(body_word_count), 1)                         AS avg_word_count,
    ROUND(AVG(link_count), 2)                              AS avg_link_count
FROM FactEmail;


-- Spam distribution by sender domain
DROP VIEW IF EXISTS v_spam_by_domain;
CREATE VIEW v_spam_by_domain AS
SELECT
    s.domain,
    s.is_internal,
    COUNT(*)                                               AS total_emails,
    SUM(CASE WHEN f.is_spam = 1 THEN 1 ELSE 0 END)         AS spam_count,
    SUM(CASE WHEN f.is_spam = 0 THEN 1 ELSE 0 END)         AS ham_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_spam = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN f.is_spam IS NOT NULL THEN 1 ELSE 0 END), 0),
        2
    )                                                      AS spam_rate_pct,
    ROUND(AVG(f.body_word_count), 1)                       AS avg_word_count,
    ROUND(AVG(f.link_count), 2)                            AS avg_link_count
FROM FactEmail f
JOIN DimSender  s ON s.sender_key = f.sender_key
GROUP BY s.domain, s.is_internal
HAVING COUNT(*) >= 3;


-- Weekday / weekend breakdown
DROP VIEW IF EXISTS v_spam_by_weekday;
CREATE VIEW v_spam_by_weekday AS
SELECT
    d.day_name,
    d.day_of_week,
    d.is_weekend,
    COUNT(*)                                               AS total_emails,
    SUM(CASE WHEN f.is_spam = 1 THEN 1 ELSE 0 END)         AS spam_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_spam = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN f.is_spam IS NOT NULL THEN 1 ELSE 0 END), 0),
        2
    )                                                      AS spam_rate_pct
FROM FactEmail f
JOIN DimDate d ON d.date_key = f.date_key
GROUP BY d.day_name, d.day_of_week, d.is_weekend
ORDER BY d.day_of_week;


-- Top 20 most active senders
DROP VIEW IF EXISTS v_top_senders;
CREATE VIEW v_top_senders AS
SELECT
    s.email_address,
    s.domain,
    s.is_internal,
    COUNT(*)                                               AS total_emails,
    SUM(CASE WHEN f.is_spam = 1 THEN 1 ELSE 0 END)         AS spam_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_spam = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN f.is_spam IS NOT NULL THEN 1 ELSE 0 END), 0),
        2
    )                                                      AS spam_rate_pct
FROM FactEmail f
JOIN DimSender s ON s.sender_key = f.sender_key
GROUP BY s.sender_key
ORDER BY total_emails DESC
LIMIT 20;
