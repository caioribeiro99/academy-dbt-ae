WITH
    motivo_venda as (
        SELECT *
        FROM {{ ref('stg_erp__motivo_de_venda') }}
    )

    , transformacoes as (
        SELECT 
            row_number() OVER (ORDER BY id_motivo_venda) as sk_motivo_venda
            , *
        FROM motivo_venda
    )

SELECT *
FROM transformacoes
ORDER BY sk_motivo_venda