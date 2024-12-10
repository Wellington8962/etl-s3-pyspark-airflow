import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# Diretório onde a DAG está localizada
dag_dir = os.path.dirname(os.path.abspath(__file__))

# Diretório de scripts de forma dinâmica
scripts_dir = os.path.abspath(os.path.join(dag_dir, "../scripts"))

# Interpretador Python do ambiente virtual
python_path = sys.executable

# Recuperar credenciais da conexão configurada no Airflow
aws_conn = BaseHook.get_connection('aws_default')
aws_access_key = aws_conn.login  # Login contém o AWS_ACCESS_KEY_ID
aws_secret_key = aws_conn.password  # Password contém o AWS_SECRET_ACCESS_KEY
aws_session_token = Variable.get("AWS_SESSION_TOKEN", default_var="")  # Token da sessão 

# Configuração padrão para as tarefas
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Base do comando spark-submit
spark_submit_base = f"""
export AWS_ACCESS_KEY_ID={aws_access_key};
export AWS_SECRET_ACCESS_KEY={aws_secret_key};
export AWS_SESSION_TOKEN={aws_session_token};
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.508 \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
--conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
--conf spark.hadoop.fs.s3a.region=us-east-1 \
--conf spark.pyspark.python={python_path} \
--conf spark.pyspark.driver.python={python_path}
"""

# Definição do DAG
with DAG(
    dag_id="etl_s3_lab",
    default_args=default_args,
    description="Orquestração do ETL no S3 com PySpark",
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Caminhos dos scripts
    gerador_senhas_path = os.path.join(scripts_dir, "gerador_senhas.py")
    processar_senhas_path = os.path.join(scripts_dir, "processar_senhas.py")
    consolidar_arquivos_path = os.path.join(scripts_dir, "consolidar_arquivos.py")

    # Etapa 1: Gerar senhas
    generate_passwords = BashOperator(
        task_id="generate_passwords",
        bash_command=f"""
        {spark_submit_base} {gerador_senhas_path}
        """,
    )

    # Etapa 2: Processar senhas
    process_passwords = BashOperator(
        task_id="process_passwords",
        bash_command=f"""
        {spark_submit_base} {processar_senhas_path}
        """,
    )

    # Etapa 3: Consolidar os arquivos
    consolidate_files = BashOperator(
        task_id="consolidate_files",
        bash_command=f"""
        {spark_submit_base} {consolidar_arquivos_path}
        """,
    )

    # Sequência de execução
    generate_passwords >> process_passwords >> consolidate_files
