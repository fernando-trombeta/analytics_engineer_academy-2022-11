with
    clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    )

    , transformed as (
        select
            row_number() over (order by id_cliente) as sk_cliente
            , id_cliente
            , nome_do_cliente
            , titulo_de_cortesia
            , nome_da_empresa
            , endereco
            , cep
            , cidade
            , regiao
            , pais
            , fax
            , telefone
        from clientes
    )

select *
from transformed