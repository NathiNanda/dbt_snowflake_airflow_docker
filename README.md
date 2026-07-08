# 💸 Exchange Rate Pipeline: AWS S3 + Snowflake + dbt-snowflake + Apache Airflow (Docker)

Este projeto é focado na transição do meu projeto local [ETL Python DuckDB DBT] para a nuvem, armazenamento semiestruturado e orquestração de pipelines de dados ponta a ponta. 

O objetivo é capturar cotações diárias de moedas (USD, EUR e BTC) da **AwesomeAPI**, armazená-las como arquivos brutos em um Data Lake no **AWS S3**, carregá-las de forma incremental em uma tabela RAW no **Snowflake** (usando o tipo `VARIANT`) e realizar a modelagem de dados e testes de qualidade via **dbt-snowflake**, tudo orquestrado automaticamente pelo **Apache Airflow** rodando em **Docker**.

---

## 🏗️ Arquitetura do Pipeline

O fluxo de dados segue uma abordagem **ELT (Extract, Load, Transform)** moderna:

<img width="850" height="748" alt="image" src="https://github.com/user-attachments/assets/b610ae09-1e3e-4565-8232-312de5f2df50" />

## AWS S3
<img width="1907" height="575" alt="image" src="https://github.com/user-attachments/assets/800d094e-6ca3-4dd7-b793-a3a70c2b2538" />

## Snowflake
<img width="1915" height="666" alt="image" src="https://github.com/user-attachments/assets/7aa98e14-a5f7-47fd-8631-cc4efc2421ce" />

## Docker
<img width="1907" height="1007" alt="image" src="https://github.com/user-attachments/assets/69bc74b0-a6dd-437f-abac-e224ad672419" />

## Airflow
<img width="1917" height="848" alt="image" src="https://github.com/user-attachments/assets/47c9d5ce-3b3f-4594-9aaa-65ff96794a10" />

---

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow 3.3.0 (Docker Compose)
* **Ingestão & Carga (Extract & Load):** Python 3.12 (`boto3` para AWS S3 e `requests`)
* **Armazenamento de Arquivos (Data Lake):** AWS S3
* **Data Warehouse:** Snowflake (com suporte a dados semiestruturados)
* **Transformação (Transform):** dbt Core 1.11.x (`dbt-snowflake`)
* **Gerenciamento de Dependências:** `uv` (gerenciador rápido de pacotes Python)

---

## 🎯 Boas Práticas e Aprendizados Implementados

1. **Abordagem ELT (vs ETL):** O pipeline extrai e carrega o JSON bruto no Snowflake sem tratamento prévio na coluna tipo `VARIANT`. Isso garante que mudanças na API não quebrem o carregamento e todo o histórico seja preservado. A transformação é delegada ao dbt diretamente no banco.
2. **Qualidade dos Dados na Origem:** Tratamos uma particularidade da AwesomeAPI diretamente na extração Python, garantindo que as metadados de moedas (`code` e `codein`) fossem propagados para todos os registros históricos do array JSON antes de enviar ao S3.
3. **Idempotência e Deduplicação no dbt:** Usamos a cláusula `QUALIFY` com `ROW_NUMBER() OVER` no Snowflake para filtrar automaticamente qualquer registro duplicado na tabela RAW, garantindo que execuções repetidas da DAG não dupliquem dados na camada final.
4. **Segurança de Credenciais:** Nenhuma chave AWS ou senha do Snowflake está exposta no código. Todas as variáveis sensíveis são salvas localmente no arquivo `.env` (que está configurado no `.gitignore`) e injetadas de forma dinâmica nos contêineres.
5. **Portabilidade do dbt:** O arquivo `profiles.yml` está contido dentro da própria pasta do projeto dbt (`dbt_project/`) e lê as variáveis via `env_var()`, eliminando a necessidade de arquivos de profiles globais.

---

## 🚀 Como Executar o Projeto Localmente

### Pré-requisitos
* Possuir o **Docker** e o **Docker Compose** instalados e rodando.
* Possuir uma conta ativa na **AWS** e um bucket S3 criado.
* Possuir uma conta ativa no **Snowflake**.

### Passo 1: Configuração das Chaves
Crie o arquivo `.env` na raiz do projeto seguindo o modelo:
```bash
# AWS
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
AWS_DEFAULT_REGION=us-east-1
AWS_BUCKET_NAME=seu-bucket-s3

# Snowflake
SNOWFLAKE_ACCOUNT=seu_identificador_de_conta.region
SNOWFLAKE_USER=seu_usuario
SNOWFLAKE_PASSWORD=sua_senha
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CURRENCY_DB
SNOWFLAKE_SCHEMA=RAW
```

### Passo 2: Configuração Inicial no Snowflake
Execute o script SQL contido em [`src/snowflake_setup.sql`](src/snowflake_setup.sql) na console do seu Snowflake (Worksheets). Lembre-se de substituir os placeholders `'COLE_SEU_AWS_ACCESS_KEY_ID_AQUI'` pelas suas chaves AWS correspondentes no navegador.

### Passo 3: Inicializando o Airflow
No terminal na raiz do projeto, execute:

1. **Construir a imagem customizada com dbt:**
   ```bash
   docker compose build --no-cache
   ```
2. **Inicializar o banco de dados interno do Airflow:**
   ```bash
   docker compose up airflow-init
   ```
3. **Subir os serviços em segundo plano:**
   ```bash
   docker compose up -d
   ```

Acesse o painel do Airflow no navegador: **`http://localhost:8080`** (Login: `airflow` / Senha: `airflow`).

### Passo 4: Executando o Pipeline
1. No painel do Airflow, localize a DAG `exchange_rate_pipeline`.
2. Ative-a clicando no interruptor azul (Unpause).
3. Clique em **Trigger DAG** (ícone de "Play" no canto superior direito) para disparar a execução.
4. Acompanhe a execução sequencial. Com todas as tarefas em verde, verifique suas tabelas diretamente no Snowflake!
