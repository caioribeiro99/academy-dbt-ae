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

    , adicionar_motivo_venda_generico as (
        SELECT
            99 as sk_motivo_venda
            , 999 as id_motivo_venda
            , 'Não especificado' as motivo_venda
            , 'Não especificado' as categoria_motivo_venda
            , cast('2008-04-30' as timestamp) as data_ultima_atualizacao
    )

SELECT *
FROM transformacoes
UNION ALL
SELECT *
FROM adicionar_motivo_venda_generico
ORDER BY sk_motivo_venda