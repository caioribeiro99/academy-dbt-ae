WITH
    detalhe_pedidos as (
        SELECT *
        FROM {{ ref('stg_erp__detalhe_pedido') }}
    )

    , produtos as (
        SELECT *
        FROM {{ ref('stg_erp__produtos') }}
    )

    , join_tabelas as (
        SELECT
            produtos.id_produto
            , produtos.id_subcategoria
            , detalhe_pedidos.id_pedido
            , detalhe_pedidos.id_detalhe_pedido
            , detalhe_pedidos.id_oferta
            , produtos.nome_produto
            , produtos.sku_produto
            , produtos.flag_produto_acabado
            , produtos.cor_produto
            , produtos.nivel_estoque_seguranca_minimo
            , produtos.ponto_de_pedido
            , produtos.custo_padrao
            , produtos.preco_venda_sugerido
            , produtos.tamanho_produto
            , produtos.unidade_medida_tamanho_produto
            , produtos.peso_produto
            , produtos.unidade_medida_peso_produto
            , produtos.dias_para_producao_produto
            , produtos.linha_produto
            , produtos.classe_produto
            , produtos.estilo_produto
            , produtos.data_inicio_venda
            , produtos.data_fim_venda
            , produtos.data_descontinuacao_produto
            , detalhe_pedidos.codigo_rastreamento_frete
            , detalhe_pedidos.qtde_pedido
            , detalhe_pedidos.preco_unitario
            , detalhe_pedidos.desconto_preco_unitario
        FROM produtos
        LEFT JOIN detalhe_pedidos
        ON produtos.id_produto = detalhe_pedidos.id_produto
    )

    , transformacoes as (
        SELECT
            row_number() OVER (ORDER BY id_produto) as sk_produto
            , *
        FROM join_tabelas
    )

SELECT *
FROM transformacoes
ORDER BY sk_produto