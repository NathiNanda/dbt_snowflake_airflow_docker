-- stg_exchange_rates.sql
-- Este modelo faz o parsing dos dados brutos JSON da tabela raw_exchange_rates no Snowflake

with raw_source as (
    select
        -- Acessa os atributos JSON dentro da coluna VARIANT 'raw_data'
        raw_data:code::string as codigo_moeda,
        raw_data:high::double as valor_maximo,
        raw_data:low::double as valor_minimo,
        raw_data:varBid::double as variacao_bid,
        raw_data:pctChange::double as porcentagem_variacao,
        raw_data:bid::double as valor_compra,
        raw_data:ask::double as valor_venda,
        raw_data:timestamp::bigint as quote_timestamp
    from {{ source('raw_data', 'raw_exchange_rates') }}
)

select
    -- 1. Chave primária (surrogate key composta via MD5 hash)
    md5(concat_ws('-', codigo_moeda, cast(quote_timestamp as varchar))) as id_cotacao,
    
    -- 2. Atributos da cotação
    codigo_moeda,
    valor_maximo,
    valor_minimo,
    variacao_bid,
    porcentagem_variacao,
    valor_compra,
    valor_venda,
    
    -- 3. Datas e Timestamps
    to_timestamp(quote_timestamp) as data_criacao
from raw_source
