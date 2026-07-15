-- =============================================================================
-- Bronze Layer — Card Spend Streaming Ingestion
-- =============================================================================
-- Purpose:
--   Ingests raw JSON transaction events landed by the
--   synthetic transaction stream generator into a Delta streaming table.
--   This is the bronze layer of the medallion architecture: an untransformed,
--   append-only record of every event exactly as it arrived.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_layer
SCHEDULE REFRESH EVERY 24 HOURS
AS
SELECT
  *,
  -- File-level lineage metadata, useful for debugging and data provenance
  _metadata.file_name              AS source_file_name,
  _metadata.file_modification_time AS ingestion_time,
  current_timestamp()              AS ingested_at

FROM STREAM read_files(
  '/Volumes/rewards_catalog/loyalty/landing/card_spend_landing/',
  format => 'json'
);


-- -----------------------------------------------------------------------------
-- Sanity check: confirm the streaming table is populated and browsable
-- -----------------------------------------------------------------------------
SELECT * FROM bronze_layer;
