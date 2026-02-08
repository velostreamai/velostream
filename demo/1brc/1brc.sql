-- SQL Application: 1brc
-- Version: 1.0.0
-- Description: One Billion Row Challenge — Velostream Edition
-- =============================================================================
-- 1BRC: Compute MIN/AVG/MAX temperature per weather station
-- =============================================================================
--
-- Reads station;temperature data from measurements.txt via memory-mapped I/O,
-- computes MIN/AVG/MAX per weather station, and writes results to CSV.
--
-- Usage:
--   velo-1brc generate --rows 1 --output measurements.txt
--   velo-test run demo/1brc/1brc.sql -y

CREATE STREAM results AS
SELECT
    station,
    MIN(temperature) AS min_temp,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp
FROM measurements
GROUP BY station
EMIT CHANGES
WITH (
    'measurements.type' = 'file_source_mmap',
    'measurements.path' = './measurements.txt',
    'measurements.format' = 'csv',
    'measurements.delimiter' = ';',
    'results.type' = 'file_sink',
    'results.path' = './1brc_results.csv',
    'results.format' = 'csv'
);
