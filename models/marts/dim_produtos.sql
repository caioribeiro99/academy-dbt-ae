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
        FROM produtos
        LEFT JOIN detalhe_pedidos
        ON produtos.id_produto = detalhe_pedidos.id_produto
    )

    , transformacoes as (
        SELECT
            row_number() OVER (ORDER BY id_produto) as sk_produto
            , id_produto
            , id_subcategoria
            , id_pedido
            , id_detalhe_pedido
            , id_oferta
            , nome_produto
            , sku_produto
            , flag_produto_acabado
            , cor_produto
            , nivel_estoque_seguranca_minimo
            , ponto_de_pedido
            , custo_padrao
            , preco_venda_sugerido
            , tamanho_produto
            , unidade_medida_tamanho_produto
            , peso_produto
            , unidade_medida_peso_produto
            , dias_para_producao_produto
            , linha_produto
            , classe_produto
            , estilo_produto
            , data_inicio_venda
            , data_fim_venda
            , data_descontinuacao_produto
            , codigo_rastreamento_frete
            , qtde_pedido
            , preco_unitario
        FROM join_tabelas
    )

SELECT *
FROM transformacoes
ORDER BY sk_produto