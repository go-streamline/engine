-- +goose Up
CREATE TABLE processors (
                            id UUID PRIMARY KEY,
                            flow_id UUID NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
                            name TEXT NOT NULL,
                            type TEXT NOT NULL,
                            flow_order INTEGER NOT NULL,
                            max_retries INTEGER DEFAULT 3,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processors_flow_id ON processors(flow_id);
CREATE INDEX idx_processors_flow_order ON processors(flow_id, flow_order);

-- +goose Down
DROP TABLE processors;