{{
    config(
        severity = 'error'
    )
}}

WITH
    vendas_em_2011 AS (
        SELECT SUM(valor_total_negociado) AS total_vendido
        FROM {{ ref('fct_vendas') }}
        WHERE data_pedido BETWEEN '2011-01-01' AND '2011-12-31'
    )

SELECT total_vendido
FROM vendas_em_2011
WHERE total_vendido NOT BETWEEN 12646112.16 AND 12646112.17