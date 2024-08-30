-- +goose Up
CREATE TABLE processors
(
    id                  UUID PRIMARY KEY,
    flow_id             UUID    NOT NULL REFERENCES flows (id) ON DELETE CASCADE,
    name                TEXT    NOT NULL,
    type                TEXT    NOT NULL,
    flow_order          INTEGER NOT NULL,
    parent_processor_id UUID REFERENCES processors (id) ON DELETE CASCADE,
    max_retries         INTEGER                  DEFAULT 3,
    log_level           TEXT    NOT NULL         DEFAULT 'info',
    configuration       JSONB   NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processors_flow_id ON processors (flow_id);
CREATE INDEX idx_processors_flow_order ON processors (flow_id, flow_order);
CREATE INDEX idx_processors_parent_processor_id ON processors (parent_processor_id);

-- +goose Down
DROP TABLE processors;


-- todo: remove