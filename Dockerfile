FROM apache/airflow:3.3.0-python3.12

# Copia o requirements.txt da raiz para dentro do container
COPY requirements.txt /requirements.txt

# Executa a instalação dos pacotes usando o pip do usuário airflow
RUN pip install --no-cache-dir -r /requirements.txt
