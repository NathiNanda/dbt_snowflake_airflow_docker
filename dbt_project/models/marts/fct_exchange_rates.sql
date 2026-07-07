-- fct_exchange_rates.sql
-- Tabela Fato analítica contendo métricas avançadas sobre as cotações de moedas

with staging_rates as (
    select * from {{ ref('stg_exchange_rates') }}
),

analytical_rates as (
    select
        id_cotacao,
        codigo_moeda,
        valor_compra,
        valor_venda,
        valor_maximo,
        valor_minimo,
        porcentagem_variacao,
        data_criacao,
        
        -- 1. Cotação do dia anterior para comparação (Window Function LAG)
        lag(valor_compra) over (
            partition by codigo_moeda 
            order by data_criacao
        ) as valor_compra_dia_anterior,
        
        -- 2. Média móvel dos últimos 7 dias da cotação (Window Function AVG)
        avg(valor_compra) over (
            partition by codigo_moeda 
            order by data_criacao 
            rows between 6 preceding and current row
        ) as media_movel_7_dias
        
    from staging_rates
)

select
    id_cotacao,
    codigo_moeda,
    valor_compra,
    valor_compra_dia_anterior,
    
    -- 3. Cálculo da variação real em relação ao dia anterior
    case 
        when valor_compra_dia_anterior is not null 
        then ((valor_compra - valor_compra_dia_anterior) / valor_compra_dia_anterior) * 100
        else 0 
    end as variacao_percentual_diaria,
    
    media_movel_7_dias,
    valor_venda,
    valor_maximo,
    valor_minimo,
    data_criacao
from analytical_rates
order by codigo_moeda, data_criacao desc
