WITH
    motivo_de_venda as (
        SELECT 
        row_number() OVER (ORDER BY id_motivo_venda) as sk_motivo_venda
        , *
        FROM {{ ref('stg_erp__motivo_de_venda') }}
    )

SELECT *
FROM motivo_de_venda
ORDER BY sk_motivo_venda