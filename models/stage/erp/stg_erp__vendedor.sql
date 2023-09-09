WITH 
    fonte_vendedores as (
        SELECT
            cast(businessentityid as integer) as id_entidade_negocio
            , cast(territoryid as integer) as id_territorio
            , cast(salesquota as integer) as cota_vendas
            , cast(bonus as integer) as bonus_vendedor
            , cast(commissionpct as float64) as taxa_comissao
            , cast(salesytd as float64) as valor_vendas_ytd
            , cast(saleslastyear as float64) as valor_vendas_ano_passado
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'salesperson') }}
    )

SELECT *
FROM fonte_vendedores