WITH
    clientes as (
        SELECT *
        FROM {{ ref('stg_erp__clientes') }}
    )

    , pessoas as (
        SELECT *
        FROM {{ ref('stg_erp__pessoas') }}
    )

    , lojas as (
        SELECT *
        FROM {{ ref('stg_erp__lojas') }}
    )

    , join_tabelas as (
        SELECT
            clientes.id_cliente
            , clientes.id_pessoa
            , clientes.id_loja
            , clientes.id_territorio
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
            END AS nome_cliente
            , pessoas.sufixo
            , lojas.nome_loja
            , pessoas.nivel_promocao_email
            , pessoas.data_ultima_atualizacao
        FROM clientes
        LEFT JOIN pessoas
        ON clientes.id_pessoa = pessoas.id_entidade_negocio
        LEFT JOIN lojas
        ON clientes.id_loja = lojas.id_entidade_negocio
    )

    , transformacao_final as (
        SELECT
            row_number() OVER (ORDER BY id_cliente) as sk_cliente
            , id_cliente
            , id_pessoa
            , id_loja
            , id_territorio
            , CASE WHEN join_tabelas.sufixo != '' THEN
                    concat(sufixo, ' ', nome_cliente)
                ELSE
                    nome_cliente
            END AS nome_cliente
            , nome_loja
            , nivel_promocao_email
        FROM join_tabelas
    )

SELECT *
FROM transformacao_final
ORDER BY sk_cliente