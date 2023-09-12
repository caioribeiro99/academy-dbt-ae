WITH
    produtos as (
        SELECT *
        , 
        FROM {{ ref('stg_erp__produtos') }}
    )

    , join_tabelas as (
        SELECT
            produtos.id_produto
            , produtos.id_subcategoria
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
        FROM produtos
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