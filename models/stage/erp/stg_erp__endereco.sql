WITH
    fonte_endereco as (
        SELECT
            cast(addressid as integer) as id_endereco
            , cast(stateprovinceid as integer) as id_estado
            , cast(addressline1 as string) as endereco
            , cast(addressline2 as string) as endereco_complementar
            , cast(city as string) as cidade
            , cast(postalcode as string) as codigo_postal
            , cast(spatiallocation as string) as geolocalizacao
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'address') }}
    )

SELECT *
FROM fonte_endereco