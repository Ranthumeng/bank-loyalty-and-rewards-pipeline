CREATE OR REFRESH STREAMING TABLE rewards_catalog.loyalty.bronze_card_spend
AS SELECT
    *,
    _metadata.file_name              AS source_file_name,
    _metadata.file_path               AS source_file_path,
    _metadata.file_modification_time AS source_file_modified_at,
    current_timestamp()               AS ingested_at
FROM STREAM read_files(
    '/Volumes/rewards_catalog/loyalty/landing/card_spend_landing',
    format => 'json'
);

select * from rewards_catalog.loyalty.bronze_card_spend
