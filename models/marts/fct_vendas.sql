WITH
    clientes as (
        SELECT *
        FROM {{ ref('dim_clientes') }}
    )

    , produtos as (
        SELECT *
        FROM {{ ref('dim_produtos') }}
    )

    , vendedores as (
        SELECT *
        FROM {{ ref('dim_vendedores') }}
    )

    , motivo_venda as (
        SELECT *
        FROM {{ ref('dim_motivo_venda') }}
    )

    , localizacao as (
        SELECT *
        FROM {{ ref('dim_localizacao') }}
    )

    , cartao_de_credito as (
        SELECT *
        FROM  {{ ref('dim_cartao_de_credito') }}
    )

    , pedido_itens as (
        SELECT *
        FROM {{ ref('int_vendas__pedido_itens') }}
    )

    , join_tabelas as (
        SELECT
            pedido_itens.id_pedido
            , produtos.id_produto as fk_produto
            , produtos.id_subcategoria as fk_subcategoria
            , pedido_itens.id_motivo_venda
            , pedido_itens.id_endereco_cobranca
            , pedido_itens.id_cartao
            , pedido_itens.id_territorio
            , pedido_itens.id_vendedor
            , pedido_itens.id_cliente
            , pedido_itens.id_metodo_envio 
            , pedido_itens.valor_subtotal_pedido
            , pedido_itens.valor_imposto
            , pedido_itens.valor_frete
            , pedido_itens.valor_total_pedido
            , pedido_itens.natureza_pedido
            , pedido_itens.qtde_pedido
            , pedido_itens.preco_unitario
            , pedido_itens.desconto_preco_unitario
            , pedido_itens.data_pedido
            , pedido_itens.data_pagamento
            , pedido_itens.data_enviado
            , pedido_itens.status_pedido
            , clientes.nome_cliente
            , produtos.nome_produto
            , produtos.custo_padrao
            , produtos.dias_para_producao_produto
            , produtos.tipo_produto
            , produtos.linha_produto
            , produtos.classe_produto
            , produtos.estilo_produto
            , produtos.data_inicio_venda
            , produtos.data_fim_venda
            , vendedores.titulo_funcao
            , vendedores.genero_vendedor
            , vendedores.modelo_contratacao_vendedor
            , motivo_venda.motivo_venda
            , motivo_venda.categoria_motivo_venda
            , cartao_de_credito.tipo_cartao
            , localizacao.endereco
            , localizacao.codigo_postal
            , localizacao.cidade
            , localizacao.latitude
            , localizacao.longitude
            , localizacao.nome_pais
            , localizacao.pais as codigo_pais
            , localizacao.nome_estado
            , localizacao.estado as codigo_estado
        FROM pedido_itens
        LEFT JOIN produtos
            ON pedido_itens.id_produto = produtos.id_produto
        LEFT JOIN clientes
            ON pedido_itens.id_cliente = clientes.id_cliente
        LEFT JOIN vendedores
            ON pedido_itens.id_vendedor = vendedores.id_vendedor
        LEFT JOIN motivo_venda
            ON pedido_itens.id_motivo_venda = motivo_venda.id_motivo_venda
        LEFT JOIN localizacao
            ON pedido_itens.id_endereco_cobranca = localizacao.id_endereco
        LEFT JOIN cartao_de_credito
            ON pedido_itens.id_cartao = cartao_de_credito.id_cartao
    )

    , transformacoes as (
        SELECT
            {{dbt_utils.generate_surrogate_key(['id_pedido', 'fk_produto'])}} as sk_venda
            , *
            , CASE
                WHEN desconto_preco_unitario > 0 THEN TRUE
                WHEN desconto_preco_unitario = 0 THEN FALSE
                ELSE FALSE
            END AS eh_desconto
            , (qtde_pedido * preco_unitario) as valor_total_negociado
            , (qtde_pedido * preco_unitario * (1-desconto_preco_unitario)) as valot_total_negociado_liquido
        FROM join_tabelas
    )

SELECT *
FROM transformacoes

