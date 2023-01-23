-- Add migration script here
CREATE TABLE natsql (
  time TIMESTAMPTZ NOT NULL,
  subject TEXT NOT NULL,
  msg JSONB NOT NULL
)
