from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------
# Função para a camada Bronze
# ----------------------
def executar_script_bronze(**kwargs):
    script_path = "/opt/airflow/dags/tasks/bronze.py"

    try:
        python_path = "/usr/local/bin/python"
        result = subprocess.run(
            [python_path, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar o script Bronze: {e}")
        logger.error(f"Saída de erro: {e.stderr}")
        raise

# ----------------------
# Função para a camada Silver
# ----------------------
def executar_script_silver(**kwargs):
    script_path = "/opt/airflow/dags/tasks/silver.py"

    try:
        python_path = "/usr/local/bin/python"
        result = subprocess.run(
            [python_path, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar o script Silver: {e}")
        logger.error(f"Saída de erro: {e.stderr}")
        raise

# ----------------------
# Função para a camada Gold
# ----------------------
def executar_script_gold(**kwargs):
    script_path = "/opt/airflow/dags/tasks/gold.py"

    try:
        python_path = "/usr/local/bin/python"
        result = subprocess.run(
            [python_path, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar o script Gold: {e}")
        logger.error(f"Saída de erro: {e.stderr}")
        raise

# ----------------------
# Definição da DAG para Bronze
# ----------------------
with DAG(
    dag_id="bronze_task",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="DAG para executar o script da camada Bronze",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as bronze_dag:

    executar_bronze_task = PythonOperator(
        task_id="executar_bronze_script",
        python_callable=executar_script_bronze,
    )

# ----------------------
# Definição da DAG para Silver
# ----------------------
with DAG(
    dag_id="silver_task",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="DAG para executar o script da camada Silver",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as silver_dag:

    executar_silver_task = PythonOperator(
        task_id="executar_silver_script",
        python_callable=executar_script_silver,
    )

# ----------------------
# Definição da DAG para Gold
# ----------------------
with DAG(
    dag_id="gold_task",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="DAG para executar o script da camada Gold",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as gold_dag:

    executar_gold_task = PythonOperator(
        task_id="executar_gold_script",
        python_callable=executar_script_gold,
    )
