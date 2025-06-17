CREATE TABLE metadata (
                          id SERIAL PRIMARY KEY,
                          data_source TEXT NOT NULL,
                          table_name TEXT NOT NULL,
                          data JSONB NOT NULL,
                          updated_at TIMESTAMP NOT NULL DEFAULT now()
);