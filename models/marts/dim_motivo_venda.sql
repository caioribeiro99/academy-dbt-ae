WITH
    motivo_venda as (
        SELECT *
        FROM {{ ref('stg_erp__motivo_de_venda') }}
    )

    , motivo_venda_detalhe_pedido as (
        SELECT *
        FROM {{ ref('stg_erp__motivo_vendas_detalhe_pedidos') }}
    )

    , join_tabelas as (
        SELECT
            motivo_venda_detalhe_pedido.id_pedido
            , motivo_venda_detalhe_pedido.id_motivo_venda
            , motivo_venda.motivo_venda
            , motivo_venda.categoria_motivo_venda
        FROM motivo_venda_detalhe_pedido
        LEFT JOIN motivo_venda
        ON motivo_venda.id_motivo_venda = motivo_venda_detalhe_pedido.id_motivo_venda
    )

    , transformacoes as (
        SELECT 
        row_number() OVER (ORDER BY id_motivo_venda) as sk_motivo_venda
        , *
        FROM join_tabelas
    )

SELECT *
FROM transformacoes
ORDER BY sk_motivo_venda