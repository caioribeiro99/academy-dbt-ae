WITH
    fonte_vendedores as (
        SELECT *
        FROM {{ ref('stg_erp__vendedor') }}
    )
    
    , fonte_funcionarios as (
        SELECT *
        FROM {{ ref('stg_erp__funcionarios') }}
    )

    , fonte_pessoas as (
        SELECT *
        FROM {{ ref('stg_erp__pessoas') }}
    )

    , 