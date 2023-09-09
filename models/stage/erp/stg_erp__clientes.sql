WITH
    fonte_conta_cliente AS (
        SELECT
            cast(customerid as integer) as id_cliente
            , cast(personid as integer) as id_pessoa
            , cast(storeid as integer) as id_loja
            , cast(territoryid as integer) as id_territorio
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'customer') }}
    )

SELECT *
FROM fonte_conta_cliente