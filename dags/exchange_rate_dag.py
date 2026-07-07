import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Adiciona o diretório /opt/airflow ao sys.path para garantir que o script encontre a pasta 'src'
sys.path.append('/opt/airflow')

from src.ingest import extract_and_load

# Configuração dos argumentos padrões da DAG
default_args = {
    'owner': 'Nathi',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1), # Início histórico fictício
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_snowflake_copy():
    """
    Função Python executada pelo Airflow que se conecta ao Snowflake
    e dispara o comando COPY INTO para carregar os dados brutos do S3 para a tabela RAW.
    """
    import snowflake.connector
    import logging
    
    # Coleta as variáveis de ambiente passadas para o container pelo docker-compose
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")
    
    logging.info(f"Conectando ao Snowflake (Conta: {account}, Banco: {database})...")
    
    # Estabelece a conexão
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
    
    cursor = conn.cursor()
    try:
        # Executa o comando COPY INTO para carregar dados brutos
        # O Snowflake vai ler da Stage 'bucket_exchange_rates' e gravar na tabela 'raw_exchange_rates'
        copy_query = """
        COPY INTO CURRENCY_DB.raw.raw_exchange_rates (raw_data)
        FROM @CURRENCY_DB.raw.bucket_exchange_rates
        FILE_FORMAT = CURRENCY_DB.raw.json_file_format;
        """
        logging.info("Disparando comando COPY INTO no Snowflake...")
        cursor.execute(copy_query)
        conn.commit()
        logging.info("Carga concluída com sucesso no Snowflake!")
    except Exception as e:
        logging.error(f"Erro ao carregar dados no Snowflake: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

# Definição da DAG
# schedule='0 8 * * *' define a execução diária às 08:00 AM
with DAG(
    'exchange_rate_pipeline',
    default_args=default_args,
    description='Pipeline diário de taxas de câmbio: API -> S3 -> Snowflake -> dbt',
    schedule='0 8 * * *',
    catchup=False,
) as dag:

    # 1. Tarefa de Extração da API e upload para o AWS S3
    task_extract_and_load_s3 = PythonOperator(
        task_id='extract_and_load_s3',
        python_callable=extract_and_load,
    )

    # 2. Tarefa de Carga de dados (COPY INTO) do S3 para o Snowflake
    task_load_to_snowflake = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=run_snowflake_copy,
    )

    # 3. Tarefa de Transformação executando 'dbt run' no Snowflake
    # Usamos o BashOperator apontando para as pastas corretas mapeadas no contêiner
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # 4. Tarefa de Testes executando 'dbt test' para qualidade dos dados
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # Definição das dependências (Ordem do Pipeline)
    task_extract_and_load_s3 >> task_load_to_snowflake >> task_dbt_run >> task_dbt_test
