WITH
    fonte_pais as (
        SELECT
            cast(countryregioncode as string) as codigo_pais
            , cast(name as string) as `país`
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'countryregion') }}
    )

SELECT *
FROM fonte_pais