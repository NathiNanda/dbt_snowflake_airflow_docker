import requests
import logging
import os
from datetime import datetime
import json
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Tenta carregar o dotenv se estiver rodando localmente (fora do Docker)
try:
    from dotenv import load_dotenv
    from pathlib import Path
    # Localiza o arquivo .env na pasta raiz do projeto (um diretório acima da pasta 'src')
    dotenv_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=dotenv_path)
except ImportError:
    pass

# Configuração de Logging para acompanhar o progresso
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_s3_client():
    """
    Starting the client to use the AWS service
    """
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    if not aws_access_key or not aws_secret_key:
        logging.error("Credenciais AWS não encontradas no ambiente.")
        raise ValueError("AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY devem ser configuradas.")
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

def upload_to_s3(s3_client, bucket_name, key_name, data):
    """
    Transform the data into JSON and send it directly to S3
    """
    try:
        # Transform the data into JSON format
        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        
        logging.info(f"Fazendo upload para s3://{bucket_name}/{key_name}...")
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=json_data,
            ContentType="application/json"
        )
        logging.info(f"Upload concluído com sucesso!")
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error(f"Erro de credenciais AWS: {e}")
        raise e
    except Exception as e:
        logging.error(f"Falha ao enviar para o S3: {e}")
        raise e

def extract_and_load():
    """
    It extracts currency data from AwesomeAPI and save
    the raw files in JSON format to the data/raw folder.
    """

    bucket_name = os.getenv("AWS_BUCKET_NAME")
    if not bucket_name:
        logging.error("Nome do bucket S3 não encontrado no ambiente (.env).")
        raise ValueError("AWS_BUCKET_NAME deve ser configurado.")

    s3_client = get_s3_client()

    moedas = ["USD-BRL","EUR-BRL","BTC-BRL"]

    for moeda in moedas:
        url = f"https://economia.awesomeapi.com.br/json/daily/{moeda}/30"

        logging.info(f"Iniciando extração da API {url}")
        resposta = requests.get(url)

        logging.info(f"Status Code: {resposta.status_code}")
    
        if resposta.status_code != 200:
            logging.error("Status Code diferente de 200")
            raise Exception(f"Falha da extração. Status Code: {resposta.status_code}")

        dados_json = resposta.json()

        # Enriquecimento dos dados para evitar valores nulos no Snowflake
        moeda_base, moeda_destino = moeda.split('-')
        for cotacao in dados_json:
            if 'code' not in cotacao or not cotacao['code']:
                cotacao['code'] = moeda_base
            if 'codein' not in cotacao or not cotacao['codein']:
                cotacao['codein'] = moeda_destino
        
        key_name = f"{moeda}.json"
        upload_to_s3(s3_client, bucket_name, key_name, dados_json)

# The block below is for testing this file directly
if __name__ == "__main__":
    # Configure basic logging for testing
    logging.basicConfig(level=logging.INFO)
    extract_and_load()