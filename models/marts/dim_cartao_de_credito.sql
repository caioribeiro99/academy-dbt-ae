WITH
    fonte_cartao_de_credito as (
        SELECT *
        FROM {{ ref('stg_erp__cartao_de_credito') }}
    )

    , cartao_de_credito as (
        SELECT
            row_number() OVER (ORDER BY id_cartao) as sk_cartao
            , id_cartao
            , tipo_cartao
            , num_cartao
            , mes_validade_cartao
            , ano_validade_cartao
        FROM fonte_cartao_de_credito
    )

    , adicionar_cartao_generico as (
        SELECT
            100000 as sk_cartao
            , 99999 as id_cartao
            , 'Não especificado' as tipo_cartao
            , null as num_cartao
            , null as mes_validade_cartao
            , null as ano_validade_cartao
    )

SELECT *
FROM cartao_de_credito
UNION ALL
SELECT *
FROM adicionar_cartao_generico
ORDER BY sk_cartao

