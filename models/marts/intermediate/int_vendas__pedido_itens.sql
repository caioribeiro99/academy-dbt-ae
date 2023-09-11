WITH
    pedidos as (
        SELECT *
        FROM {{ ref('stg_erp__cabecalho_pedido') }}
    )

    , detalhe_pedidos as (
        SELECT *
        FROM {{ ref('stg_erp__detalhe_pedido') }}
    )

    , vendas_pedido_itens as (
        SELECT
            pedidos.id_pedido
            , pedidos.id_cliente
            , pedidos.id_vendedor
            , pedidos.id_territorio
            , pedidos.id_endereco_cobranca
            , pedidos.id_endereco_entrega
            , pedidos.id_metodo_envio
            , pedidos.id_cartao
            , pedidos.id_taxa_cambio
            , detalhe_pedidos.id_detalhe_pedido
            , detalhe_pedidos.id_produto
            , detalhe_pedidos.id_oferta
            , pedidos.codigo_aprovacao_cartao
            , pedidos.valor_subtotal_pedido
            , pedidos.valor_imposto
            , pedidos.valor_frete
            , pedidos.valor_total_pedido
            , pedidos.num_revisao
            -- , pedidos.data_pedido
            -- , pedidos.data_pagamento
            -- , pedidos.data_enviado
            , pedidos.status_pedido
            , CASE WHEN
                pedidos.flag_pedido_online is TRUE THEN 'Compra Online'
              ELSE
                'Compra em loja física'
            END AS `natureza_pedido`
            , pedidos.ordem_de_compra
            , pedidos.numero_conta
            , detalhe_pedidos.codigo_rastreamento_frete
            , detalhe_pedidos.qtde_pedido
            , detalhe_pedidos.preco_unitario
            , detalhe_pedidos.desconto_preco_unitario
        FROM detalhe_pedidos
        LEFT JOIN pedidos 
        ON detalhe_pedidos.id_pedido = pedidos.id_pedido 
    )

SELECT *
FROM vendas_pedido_itens