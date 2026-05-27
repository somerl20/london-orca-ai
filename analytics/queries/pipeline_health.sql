-- ── Pipeline Health Dashboard ───────────────────────────────────────────────

-- [Card] Total records ingested (single number)
SELECT COUNT(*) AS total_records
FROM measurements;


-- [Card] Records processed today (single number)
SELECT COUNT(*) AS records_today
FROM measurements
WHERE date = CURRENT_DATE;


-- [Card] Files processed (single number)
SELECT COUNT(*) AS files_processed
FROM processed_files;


-- [Card] Records per hour — last 24 h (time series)
SELECT
    DATE_TRUNC('hour', event_ts) AS hour,
    COUNT(*)                     AS records
FROM measurements
WHERE event_ts >= NOW() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', event_ts)
ORDER BY hour;


-- [Card] Records per day — last 30 days (bar chart)
SELECT
    date,
    COUNT(*) AS records
FROM measurements
WHERE date >= CURRENT_DATE - 30
GROUP BY date
ORDER BY date;
