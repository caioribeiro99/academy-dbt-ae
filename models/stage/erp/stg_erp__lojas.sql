WITH
    lojas as (
        SELECT
            cast(businessentityid as integer) as id_entidade_negocio
            , cast(name as string) as nome_loja
            , cast(salespersonid as integer) as id_vendedor
            , cast(demographics as string) as dados_demograficos
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        from {{ source('erp', 'store') }}
    )

SELECT *
FROM lojas