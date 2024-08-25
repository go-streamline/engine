-- +goose Up
CREATE TABLE flows (
                       id UUID PRIMARY KEY,
                       name TEXT NOT NULL,
                       flow_order INT NOT NULL,
                       created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- +goose Down
DROP TABLE flows;
