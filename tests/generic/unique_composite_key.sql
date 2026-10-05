-- tests/generic/unique_composite_key.sql

{% test unique_composite_key(model, columns) %}

SELECT
    {{ columns | join(', ') }},
    COUNT(*) AS row_count
FROM {{ model }}
GROUP BY
    {{ columns | join(', ') }}
HAVING COUNT(*) > 1

{% endtest %}