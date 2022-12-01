with
    categorias as (
        select *
        from {{ ref('stg_erp__categorias') }}
    )

    , produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )

    , fornecedores as (
        select *
        from {{ ref('stg_erp__fornecedores') }}
    )  
    --abaixo foi criado um join para as 3 tabelas acima

    , uniao_tabelas as (
        select 
            produtos.id_produto
            , produtos.id_fornecedor	
            , produtos.id_categoria	
            , produtos.nome_produto			
            , produtos.quantidade_por_unidade
            , produtos.preco_por_unidade
            , produtos.unidades_em_estoque		
            , produtos.unidades_por_ordem			
            , produtos.nivel_reabastecimento		
            , produtos.is_descontinuado
            , categorias.nome_categoria			
            , categorias.descricao_categoria
            , fornecedores.nome_do_fornecedor
            , fornecedores.contato_fornecedor
            , fornecedores.titulo_de_cortesia_fornecedor
            , fornecedores.endereco_fornecedor
            , fornecedores.cep_fornecedor
            , fornecedores.cidade_fornecedor
            , fornecedores.regiao_fornecedor
            , fornecedores.pais_fornecedor
            , fornecedores.fax_fornecedor
            , fornecedores.telefone_fornecedor
        from produtos
        left join categorias on produtos.id_categoria = categorias.id_categoria
        left join fornecedores on produtos.id_fornecedor = fornecedores.id_fornecedor
    )

    , transformacoes as (
        select
            row_number() over (order by id_produto) as sk_produto
            , *
        from uniao_tabelas
    )

    select *
    from transformacoes