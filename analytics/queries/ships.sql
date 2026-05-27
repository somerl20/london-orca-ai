-- ── Ships Dashboard ──────────────────────────────────────────────────────────

-- [Card] Active ships today (single number)
SELECT COUNT(DISTINCT source_id) AS active_ships_today
FROM measurements
WHERE date = CURRENT_DATE;


-- [Card] Active ships per day — last 30 days (time series)
SELECT
    date,
    COUNT(DISTINCT source_id) AS active_ships
FROM measurements
WHERE date >= CURRENT_DATE - 30
GROUP BY date
ORDER BY date;


-- [Card] Top ships by record count — last 30 days (horizontal bar)
SELECT
    source_id,
    COUNT(*) AS records
FROM measurements
WHERE date >= CURRENT_DATE - 30
GROUP BY source_id
ORDER BY records DESC
LIMIT 20;


-- [Card] Latest position per ship (table)
SELECT
    source_id,
    MAX(event_ts)            AS last_seen,
    ROUND(AVG(latitude),  4) AS latitude,
    ROUND(AVG(longitude), 4) AS longitude,
    ROUND(AVG(speed_knots), 1) AS avg_speed_knots
FROM measurements
WHERE date >= CURRENT_DATE - 1
GROUP BY source_id
ORDER BY last_seen DESC;
