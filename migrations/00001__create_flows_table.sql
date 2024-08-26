-- +goose Up
CREATE TABLE flows (
                       id UUID PRIMARY KEY,
                       name TEXT NOT NULL,
                       description TEXT,
                       created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                       updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
);

-- +goose Down
DROP TABLE flows;
