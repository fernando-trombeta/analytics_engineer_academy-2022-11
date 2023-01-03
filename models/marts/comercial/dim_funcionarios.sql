with
    funcionarios as (
        select *
        from {{ ref('stg_erp__funcionarios') }}
    )

    , self_joined as (
        select
            funcionarios.id_funcionario
            , funcionarios.id_gerente
            , funcionarios.nome_do_funcionario
            , funcionarios.sobrenome_do_funcionario
            , funcionarios.nome_completo_funcionario
            , funcionarios.titulo_de_cortesia
            , funcionarios.data_de_nascimento
            , funcionarios.cargo
            , funcionarios.data_da_contratacao
            , gerentes.nome_completo_funcionario as gerente
            , funcionarios.endereco
            , funcionarios.cep
            , funcionarios.cidade
            , funcionarios.regiao
            , funcionarios.pais
            , funcionarios.telefone
            , funcionarios.observacoes
        from funcionarios
        left join funcionarios as gerentes on
            funcionarios.id_gerente = gerentes.id_funcionario
    )

    , transformed as (
        select
            row_number() over (order by id_funcionario) as sk_funcionario
            , *
        from self_joined
    )

select *
from transformed