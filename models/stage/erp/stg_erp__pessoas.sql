WITH
    fonte_pessoas as (
        SELECT
            cast(businessentityid as integer) as id_entidade_negocio
            , cast(persontype as string) as tipo_pessoa
            , cast(namestyle as boolean) as estilo_nome
            , cast(title as string) as titulo
            , cast(firstname as string) as primeiro_nome
            , cast(middlename as string) as nome_do_meio
            , cast(lastname as string) as ultimo_nome
            , cast(suffix as string) as sufixo
            , cast(emailpromotion as integer) as nivel_promocao_email
            , cast(additionalcontactinfo as string) as infos_adicionais_contato
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'person') }}    
    )

SELECT *
FROM fonte_pessoas