WITH
    fonte_funcionarios as (
        SELECT
            cast(businessentityid as integer) as id_entidade_negocio
            , cast(nationalidnumber as integer) as id_nacionalidade
            , cast(jobtitle as string) as titulo_funcao
            , cast(gender as string) as genero_funcionario
            , cast(hiredate as date) as data_contratacao
            , cast(salariedflag as boolean) as modelo_contratacao
            , cast(currentflag as boolean) as funcionario_ativo
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'employee') }}
    )

SELECT *
FROM fonte_funcionarios