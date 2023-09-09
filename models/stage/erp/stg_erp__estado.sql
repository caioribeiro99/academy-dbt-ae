WITH
    fonte_estado as (
        SELECT
            cast(stateprovinceid as integer) as id_estado
            , cast(territoryid as integer) as id_territorio
            , cast(stateprovincecode as string) as estado
            , cast(countryregioncode as string) as `país`
            , cast(isonlystateprovinceflag as boolean) as traz_apenas_estado
            , cast(name as string) as stateprovince_name
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'stateprovince') }}
    )

SELECT *
FROM fonte_estado