-- History of every data quality check, so quality can be tracked over time
-- ("how often did completeness fail last month?"), not only in today's log.

CREATE TABLE analytics.dq_check_results (
    result_id     BIGSERIAL   PRIMARY KEY,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_start  DATE        NOT NULL,
    window_end    DATE        NOT NULL,
    check_name    TEXT        NOT NULL,
    severity      TEXT        NOT NULL CHECK (severity IN ('error', 'warning')),
    passed        BOOLEAN     NOT NULL,
    failing_rows  INTEGER     NOT NULL,
    details       TEXT
);
