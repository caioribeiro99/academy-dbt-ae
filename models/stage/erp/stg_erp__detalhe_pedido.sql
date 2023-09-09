WITH
    fonte_detalhe_pedidos as (
        SELECT
            cast(salesorderid as integer) as id_pedido
            , cast(salesorderdetailid as integer) as id_detalhe_pedido
            , cast(productid as integer) as id_produto
            , cast(specialofferid as integer) as id_oferta
            , cast(carriertrackingnumber as string) as codigo_rastreamento_frete
            , cast(orderqty as integer) as qtde_pedido
            , cast(unitprice as float64) as preco_unitario
            , cast(unitpricediscount as float64) as desconto_preco_unitario
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'salesorderdetail') }}
    )

SELECT *
FROM fonte_detalhe_pedidos