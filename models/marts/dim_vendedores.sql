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
            , CASE 
                WHEN funcionarios.modelo_contratacao = TRUE THEN 'Assalariado'
              ELSE
                'Horista'
            END AS modelo_contratacao
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
            , id_entidade_negocio as id_vendedor
            , id_nacionalidade
            , id_territorio
            , titulo_funcao
            , CASE
                WHEN join_tabelas.sufixo != '' THEN
                    concat(join_tabelas.sufixo, ' ', join_tabelas.nome_vendedor)
                ELSE
                    join_tabelas.nome_vendedor
            END AS nome_vendedor
            , genero_funcionario as genero_vendedor
            , data_contratacao
            , modelo_contratacao as modelo_contratacao_vendedor
            , funcionario_ativo
            , tipo_pessoa
            , nivel_promocao_email
            , cota_vendas
            , bonus_vendedor
            , taxa_comissao as taxa_comissao_vendedor
            , valor_vendas_ano_passado
            , valor_vendas_ytd
        FROM join_tabelas
    )

    , adicionar_vendedor_generico as (
        SELECT
            99 as sk_vendedor
            , 999 as id_vendedor
            , null as id_nacionalidade
            , null as id_territorio
            , 'Sales Representative' as titulo_funcao
            , 'Vendedor Não Identificado' as nome_vendedor
            , 'NAN' as genero_vendedor
            , cast('2011-01-01' as date) as data_contratacao
            , 'NAN' as modelo_contratacao
            , true as funcionario_ativo
            , 'SP' as tipo_pessoa
            , 0 as nivel_promocao_email
            , null as cota_vendas
            , null as bonus_vendedor
            , null as taxa_comissao_vendedor
            , null as valor_vendas_ano_passado
            , null as valor_vendas_ytd
    )

SELECT *
FROM transformacao_final
UNION ALL
SELECT *
FROM adicionar_vendedor_generico
ORDER BY sk_vendedor