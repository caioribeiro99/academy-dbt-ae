WITH
    endereco as (
        SELECT *
        FROM {{ ref('stg_erp__endereco') }}
    )

    , estado as (
        SELECT *
        FROM {{ ref('stg_erp__estado') }}
    )

    , pais as (
        SELECT *
        FROM {{ ref('stg_erp__pais') }}
    )

    , join_tabelas as (
        SELECT
            endereco.id_endereco
            , endereco.id_estado
            , estado.id_territorio
            , endereco.endereco
            , endereco.endereco_complementar
            , endereco.cidade
            , endereco.codigo_postal
            , endereco.geolocalizacao
            , estado.estado
            , estado.`país`
            , estado.traz_apenas_estado
            , estado.nome_estado
            , pais.nome_pais
        FROM endereco
        LEFT JOIN estado
        ON endereco.id_estado = estado.id_estado
        LEFT JOIN pais
        ON estado.`país` = pais.codigo_pais
    )

    , transformacao_final as (
        SELECT
            row_number() OVER (ORDER BY id_endereco) as sk_localizacao
            , id_endereco
            , id_estado
            , id_territorio
            , CASE WHEN endereco_complementar != '' THEN
                concat(endereco, ' ', endereco_complementar)
              ELSE
                coalesce(endereco, endereco_complementar)
            END AS endereco
            , cidade
            , codigo_postal
            , CAST(CONCAT('0x', SUBSTR(geolocalizacao, 3, 16)) AS FLOAT64) / 1000000.0 AS latitude
            , CAST(CONCAT('0x', SUBSTR(geolocalizacao, 19, 16)) AS FLOAT64) / 1000000.0 AS longitude
            , estado
            , nome_estado
            , `país` as pais
            , nome_pais
        FROM join_tabelas
    )

SELECT *
FROM transformacao_final
ORDER BY sk_localizacao