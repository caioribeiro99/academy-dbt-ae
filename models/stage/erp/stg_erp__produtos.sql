WITH
    fonte_produtos as (
        SELECT
            cast(productid as integer) as id_produto
            , cast(productsubcategoryid as integer) as id_subcategoria
            , cast(name as string) as nome_produto
            , cast(productnumber as string) as sku_produto
            , cast(makeflag as boolean) as flag_produto_interno
            , cast(finishedgoodsflag as boolean) as flag_produto_acabado
            , cast(color as string) as cor_produto
            , cast(safetystocklevel as integer) as nivel_estoque_seguranca_minimo
            , cast(reorderpoint as integer) as ponto_de_pedido
            , cast(standardcost as float64) as custo_padrao
            , cast(listprice as float64) as preco_venda_sugerido
            , cast(`size` as string) as tamanho_produto
            , cast(sizeunitmeasurecode as string) as unidade_medida_tamanho_produto
            , cast(weight as float64) as peso_produto
            , cast(weightunitmeasurecode as string) as unidade_medida_peso_produto
            , cast(daystomanufacture as integer) as dias_para_producao_produto
            , cast(productline as string) as linha_produto
            , cast(class as string) as classe_produto
            , cast(style as string) as estilo_produto
            , cast(sellstartdate as timestamp) as data_inicio_venda
            , cast(sellenddate as timestamp) as data_fim_venda
            , cast(discontinueddate as integer) as data_descontinuacao_produto
            , cast(modifieddate as timestamp) as data_ultima_atualizacao
        FROM {{ source('erp', 'product') }}
    )

SELECT *
FROM fonte_produtos