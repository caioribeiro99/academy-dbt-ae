WITH
    vendedores as (
        SELECT *
        FROM {{ ref('stg_erp__vendedor') }}
    )
    
    , funcionarios as (
        SELECT *
        FROM {{ ref('stg_erp__funcionarios') }}
    )

    , pessoas as (
        SELECT *
        FROM {{ ref('stg_erp__pessoas') }}
    )

    , join_tabelas as (
        SELECT
            funcionarios.id_entidade_negocio
            , funcionarios.id_nacionalidade
            , vendedores.id_territorio
            , funcionarios.titulo_funcao
            , funcionarios.genero_funcionario
            , funcionarios.data_contratacao
            , funcionarios.modelo_contratacao
            , funcionarios.funcionario_ativo
            , pessoas.tipo_pessoa
            , CASE
                WHEN 
                    pessoas.estilo_nome = TRUE AND pessoas.titulo = '' AND pessoas.nome_do_meio != '' THEN 
                    concat(pessoas.ultimo_nome, ' ', pessoas.nome_do_meio , ' ', pessoas.primeiro_nome)
                WHEN 
                    pessoas.estilo_nome = TRUE AND pessoas.titulo != '' AND pessoas.nome_do_meio != '' THEN
                    concat(pessoas.titulo, ' ', pessoas.ultimo_nome, ' ', pessoas.nome_do_meio, ' ', pessoas.primeiro_nome)
                WHEN 
                    pessoas.estilo_nome = FALSE AND pessoas.titulo = '' AND pessoas.nome_do_meio != '' THEN 
                    concat(pessoas.primeiro_nome, ' ', pessoas.nome_do_meio, ' ', pessoas.ultimo_nome)
                WHEN
                    pessoas.estilo_nome = FALSE AND pessoas.titulo != '' AND pessoas.nome_do_meio != '' THEN 
                    concat(pessoas.titulo, ' ', pessoas.primeiro_nome, ' ', pessoas.nome_do_meio, ' ', pessoas.ultimo_nome)
                WHEN 
                    pessoas.estilo_nome = TRUE AND pessoas.titulo = '' AND pessoas.nome_do_meio = '' THEN 
                    concat(pessoas.ultimo_nome, ' ', pessoas.primeiro_nome)
                WHEN 
                    pessoas.estilo_nome = TRUE AND pessoas.titulo != '' AND pessoas.nome_do_meio = '' THEN
                    concat(pessoas.titulo, ' ', pessoas.ultimo_nome, ' ', pessoas.primeiro_nome)
                WHEN 
                    pessoas.estilo_nome = FALSE AND pessoas.titulo = '' AND pessoas.nome_do_meio = '' THEN 
                    concat(pessoas.primeiro_nome, ' ', pessoas.ultimo_nome)
                WHEN
                    pessoas.estilo_nome = FALSE AND pessoas.titulo != '' AND pessoas.nome_do_meio = '' THEN 
                    concat(pessoas.titulo, ' ', pessoas.primeiro_nome, ' ', pessoas.ultimo_nome)
                ELSE
                    coalesce(pessoas.primeiro_nome, pessoas.ultimo_nome)
            END AS nome_vendedor
            , pessoas.sufixo
            , pessoas.nivel_promocao_email
            , vendedores.cota_vendas
            , vendedores.bonus_vendedor
            , vendedores.taxa_comissao
            , vendedores.valor_vendas_ytd
            , vendedores.valor_vendas_ano_passado
        FROM vendedores
        LEFT JOIN funcionarios
        ON vendedores.id_entidade_negocio = funcionarios.id_entidade_negocio
        LEFT JOIN pessoas
        ON vendedores.id_entidade_negocio = pessoas.id_entidade_negocio
    )

    , transformacao_final as (
        SELECT
            row_number() OVER (ORDER BY id_entidade_negocio) as sk_vendedor
            , id_entidade_negocio
            , id_nacionalidade
            , id_territorio
            , titulo_funcao
            , CASE
                WHEN join_tabelas.sufixo != '' THEN
                    concat(join_tabelas.sufixo, ' ', join_tabelas.nome_vendedor)
                ELSE
                    join_tabelas.nome_vendedor
            END AS nome_vendedor
            , genero_funcionario
            , data_contratacao
            , modelo_contratacao
            , funcionario_ativo
            , tipo_pessoa
        FROM join_tabelas
    )

SELECT *
FROM transformacao_final
ORDER BY sk_vendedor