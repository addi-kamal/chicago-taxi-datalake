-- =============================================================================
-- Requêtes d'exemple Trino - une par KPI (catalogue `iceberg`, identique à la prod).
-- Exécution : docker compose exec trino trino --catalog iceberg
--             ou make trino-cli, puis copier-coller.
-- =============================================================================

-- 1. Revenus par jour (gold.daily_revenue)
SELECT trip_date, trips, total_revenue, avg_ticket, tip_rate
FROM iceberg.gold.daily_revenue
ORDER BY trip_date;

-- 2. Durée moyenne / médiane / p90 par heure de prise en charge (gold.trip_duration_stats)
--    durée : moyenne pondérée par le nombre de trajets de chaque (jour, heure)
--    vitesse : Σ miles / Σ heures, recalculée depuis les mesures additives
SELECT trip_hour,
       sum(trips)                                            AS trips,
       round(sum(avg_duration_min * trips) / sum(trips), 1)  AS avg_duration_min,
       round(approx_percentile(median_duration_min, 0.5), 1) AS median_of_daily_medians,
       round(sum(sum_trip_miles)
             / (CAST(sum(sum_trip_seconds) AS double) / 3600.0), 1) AS avg_speed_mph
FROM iceberg.gold.trip_duration_stats
GROUP BY trip_hour
ORDER BY trip_hour;

-- 3. Top 10 des zones de prise en charge par mois (gold.top_pickup_areas)
SELECT trip_month, area_rank, community_area_name, side, trips, total_revenue,
       round(100 * share_of_trips, 2) AS pct_trips
FROM iceberg.gold.top_pickup_areas
WHERE area_rank <= 10
ORDER BY trip_month, area_rank;

-- 4. Mix des moyens de paiement (gold.payment_mix)
SELECT payment_type, sum(trips) AS trips, sum(total_revenue) AS revenue,
       round(100.0 * sum(trips) / sum(sum(trips)) OVER (), 2) AS pct_trips
FROM iceberg.gold.payment_mix
GROUP BY payment_type
ORDER BY trips DESC;

-- 5. Performance des compagnies (gold.company_performance)
SELECT trip_month, revenue_rank, company, trips, active_taxis, total_revenue, avg_ticket, tip_rate
FROM iceberg.gold.company_performance
WHERE revenue_rank <= 10
ORDER BY trip_month, revenue_rank;

-- 6. Qualité : résultats de la dernière exécution (silver.dq_results)
SELECT rule, severity, checked_rows, failed_rows, failed_pct
FROM iceberg.silver.dq_results
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM iceberg.silver.dq_results)
ORDER BY severity, rule;

-- 7. Quarantaine : raisons de rejet
SELECT reject_reason, count(*) AS rows
FROM iceberg.silver.taxi_trips_rejected
GROUP BY reject_reason
ORDER BY rows DESC;

-- 8. Iceberg : historique des snapshots et fichiers d'une table (métadonnées)
SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added_records
FROM iceberg.silver."taxi_trips$snapshots"
ORDER BY committed_at DESC;

SELECT partition, record_count, file_count, total_size
FROM iceberg.silver."taxi_trips$partitions"
ORDER BY partition
LIMIT 10;

-- 9. Time travel : état de la table gold avant le dernier chargement
-- SELECT count(*) FROM iceberg.gold.daily_revenue FOR VERSION AS OF <snapshot_id>;
