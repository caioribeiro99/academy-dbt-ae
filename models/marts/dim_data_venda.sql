WITH
    data_venda as (
        SELECT
            row_number() OVER(ORDER BY id_pedido) as sk_data_venda
            , id_pedido
            , id_cliente
            , id_vendedor
            , id_territorio
            , id_endereco_cobranca
            , id_endereco_entrega
            , id_metodo_envio
            , id_cartao
            , id_taxa_cambio
            , data_pedido
            , data_pagamento
            , data_enviado
        FROM {{ ref('stg_erp__cabecalho_pedido') }}
    )

SELECT *
FROM data_venda
ORDER BY sk_data_venda