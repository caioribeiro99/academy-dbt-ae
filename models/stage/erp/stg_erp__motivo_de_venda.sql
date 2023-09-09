WITH
    fonte_motivo_de_venda AS (
        SELECT
            cast(salesreasonid as integer) as id_motivo_venda
            , cast(name as string) as motivo_venda
            , cast(reasontype as string) as categoria_motivo_venda
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'salesreason') }}
    )

SELECT *
FROM fonte_motivo_de_venda
    