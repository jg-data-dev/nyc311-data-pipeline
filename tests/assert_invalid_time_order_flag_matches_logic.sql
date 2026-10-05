-- tests/assert_invalid_time_order_flag_matches_logic.sql

SELECT
    unique_key,
    created_at,
    closed_at,
    is_invalid_time_order
FROM {{ ref('int_complaints_flagged') }}
WHERE
    (
        closed_at IS NOT NULL
        AND created_at IS NOT NULL
        AND closed_at < created_at
        AND is_invalid_time_order <> 1
    )
    OR
    (
        NOT (
            closed_at IS NOT NULL
            AND created_at IS NOT NULL
            AND closed_at < created_at
        )
        AND is_invalid_time_order <> 0
    )