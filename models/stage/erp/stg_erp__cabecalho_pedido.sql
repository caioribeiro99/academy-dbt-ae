WITH
    fonte_cabecalho_pedido as (
        SELECT
            cast(salesorderid as integer) as id_pedido
            , cast(customerid as integer) as id_cliente
            , cast(salespersonid as integer) as id_vendedor
            , cast(territoryid as integer) as id_territorio
            , cast(billtoaddressid as integer) as id_endereco_cobranca
            , cast(shiptoaddressid as integer) as id_endereco_entrega
            , cast(shipmethodid as integer) as id_metodo_envio
            , cast(creditcardid as integer) as id_cartao
            , cast(currencyrateid as integer) as id_taxa_cambio
            , cast(creditcardapprovalcode as string) as codigo_aprovacao_cartao
            , cast(subtotal as float64) as valor_subtotal_pedido
            , cast(taxamt as float64) as valor_imposto
            , cast(freight as float64) as valor_frete
            , cast(totaldue as float64) as valor_total_pedido
            , cast(revisionnumber as integer) as num_revisao
            , cast(orderdate as timestamp) as data_pedido
            , cast(duedate as timestamp) as data_pagamento
            , cast(shipdate as timestamp) as data_enviado
            , cast(status as integer) as status_pedido
            , cast(onlineorderflag as boolean) as flag_pedido_online
            , cast(purchaseordernumber as string) as ordem_de_compra
            , cast(accountnumber as string) as numero_conta
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
            
        FROM {{ source('erp', 'salesorderheader') }}
    )

SELECT *
FROM fonte_cabecalho_pedido