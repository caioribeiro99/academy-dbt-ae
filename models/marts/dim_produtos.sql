WITH
    fonte_produtos as (
        SELECT *
        , 
        FROM {{ ref('stg_erp__produtos') }}
    )

    , produtos as (
        SELECT
            id_produto
            , id_subcategoria
            , nome_produto
            , sku_produto
            , CASE
                WHEN flag_produto_acabado = TRUE THEN 'Produto Acabado'
              ELSE
                'Componente Intermediário'
            END AS tipo_produto
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
        FROM fonte_produtos
    )

    , transformacoes as (
        SELECT
            row_number() OVER (ORDER BY id_produto) as sk_produto
            , *
        FROM produtos
    )

SELECT *
FROM transformacoes
ORDER BY sk_produto