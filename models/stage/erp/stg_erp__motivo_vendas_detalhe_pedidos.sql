WITH
    motivo_vendas_pedido_detalhes as (
        SELECT
            cast(salesorderid as integer) as id_pedido
            , cast(salesreasonid as integer) as id_motivo_venda
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'salesorderheadersalesreason') }}
    )

SELECT *
FROM motivo_vendas_pedido_detalhes