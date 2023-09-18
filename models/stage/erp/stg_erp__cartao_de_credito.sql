WITH
    fonte_cartao_de_credito as (
        SELECT *
        FROM {{ source('erp', 'creditcard') }}
    )

    , cartao_de_credito as (
        SELECT
            cast(creditcardid as integer) as id_cartao
            , cast(cardtype as string) as tipo_cartao
            , cast(cardnumber as integer) as num_cartao
            , cast(expmonth as integer) as mes_validade_cartao
            , cast(expyear as integer) as ano_validade_cartao
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM fonte_cartao_de_credito
    )

SELECT *
FROM cartao_de_credito


