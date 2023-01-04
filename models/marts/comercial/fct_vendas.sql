with 
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , transportadoras as (
        select *
        from {{ ref('dim_transportadoras') }}
    )

-- pedido item = veio da tabela intermedária
    , pedido_item as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    /* Outra maneira de fazer (sem fazer a tabela intermediária MODULARIZAÇÃO), é trazer aqui as tabelas
       order e order_details e faria a transformação aqui completa do modelo.
       Nesse caso foi feita a tabela intermediária e chamada de "joined". */

    /* - Nesse exemplo ele pegou as colunas da intermediária.joined e renomeou de 
       pedido para pedido_item.
       - Da dim_clientes só pegou nome_cliente, mas poderia pegar todas ou qual for importante.
       - Da dim_funcionarios só pegou nome_completo_funcionario e .gerente.
       - Da dim_transportadoras só pegou nome_transportadora.
       - Da dim_produtos pegou nome_produto, nome_categoria, nome_do_fornecedor e
       is_discontinuado.
        */

    /* - Lembrar de trocar os id_ pelas sk_
       - Lembrando que elas estão como sk (chave primaria) mas na verdade
       são fk (chaves estrangeiras)... então tem que renomear elas com
       as fk_.....         */

    , joined as (
        select
            pedido_item.id_pedido
            , funcionarios.sk_funcionario as fk_funcionario
            , clientes.sk_cliente as fk_cliente
            , transportadoras.sk_transportadora as fk_transportadora
            , produtos.sk_produto as fk_produto
            , pedido_item.desconto
            , pedido_item.preco_da_unidade
            , pedido_item.quantidade
            , pedido_item.frete
            , pedido_item.data_do_pedido
            , pedido_item.data_do_envio
            , pedido_item.data_requerida
            , pedido_item.destinatario
            , pedido_item.endereco_destinatario
            , pedido_item.cep_destinatario
            , pedido_item.cidade_destinatario
            , pedido_item.regiao_destinatario
            , pedido_item.pais_destinatario
            , clientes.nome_do_cliente
            , funcionarios.nome_completo_funcionario
            , funcionarios.gerente
            , transportadoras.nome_transportadora
            , produtos.nome_produto
            , produtos.nome_categoria
            , produtos.nome_do_fornecedor
            , produtos.is_descontinuado
        from pedido_item
        left join clientes on pedido_item.id_cliente = clientes.id_cliente
        left join funcionarios on pedido_item.id_funcionario = funcionarios.id_funcionario
        left join produtos on pedido_item.id_produto = produtos.id_produto
        left join transportadoras on pedido_item.id_transportadora = transportadoras.id_transportadora
    )

-- onde são feitas a métricas:
    , transformacoes as (
        select
            *
            , case 
                when desconto > 0 then true 
                when desconto = 0 then false 
                else false
                end as is_teve_desconto
            , preco_da_unidade * quantidade as venda_total_bruto
            , (1- desconto) * preco_da_unidade * quantidade as venda_total_liquido
        from joined
    )

select *
from transformacoes


    
