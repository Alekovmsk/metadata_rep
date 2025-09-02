ALTER TABLE metadata
    ADD COLUMN record_key text NOT NULL,
    ADD COLUMN data_hash  text NOT NULL;

CREATE UNIQUE INDEX ux_metadata_key
    ON metadata(data_source, table_name, record_key);