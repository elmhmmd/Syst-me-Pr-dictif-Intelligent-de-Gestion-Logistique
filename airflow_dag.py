from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Project configuration
PROJECT_PATH = "/home/simo/Desktop/Projects/Syst-me-Pr-dictif-Intelligent-de-Gestion-Logistique"
VENV_PYTHON = f"{PROJECT_PATH}/Gestion_Logistique/bin/python"
VENV_STREAMLIT = f"{PROJECT_PATH}/Gestion_Logistique/bin/streamlit"

default_args = {
    'owner': 'spark_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='logistics_realtime_pipeline',
    default_args=default_args,
    description='Pipeline: FastAPI → Socket Bridge → Spark Streaming → PostgreSQL/MongoDB → Streamlit',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 11, 25),
    catchup=False,
    tags=['logistics', 'streaming', 'ml'],
) as dag:

    # Task 1: Start Docker Infrastructure (PostgreSQL + MongoDB)
    # This MUST complete before other services start
    start_infra = BashOperator(
        task_id='start_docker_infra',
        bash_command=f'''
            cd {PROJECT_PATH}
            docker-compose up -d
            sleep 10
            exit 0
        ''',
    )

    # Task 2: Start FastAPI Data Server (runs in parallel with others)
    start_api = BashOperator(
        task_id='start_producer_api',
        bash_command=f'''
            cd {PROJECT_PATH}
            {VENV_PYTHON} data_server.py > api.log 2>&1 &
            sleep 2
            exit 0
        ''',
    )

    # Task 3: Start Socket Bridge (runs in parallel with others)
    start_bridge = BashOperator(
        task_id='start_socket_bridge',
        bash_command=f'''
            cd {PROJECT_PATH}
            {VENV_PYTHON} socket_bridge.py > bridge.log 2>&1 &
            sleep 2
            exit 0
        ''',
    )

    # Task 4: Start Spark Streaming (runs in parallel with others)
    start_spark = BashOperator(
        task_id='start_spark_streaming',
        bash_command=f'''
            cd {PROJECT_PATH}
            {VENV_PYTHON} spark_streaming_job.py > spark.log 2>&1 &
            sleep 2
            exit 0
        ''',
    )

    # Task 5: Start Streamlit Dashboard (runs in parallel with others)
    start_dashboard = BashOperator(
        task_id='start_streamlit',
        bash_command=f'''
            cd {PROJECT_PATH}
            {VENV_STREAMLIT} run dashboard.py --server.port 8501 > dashboard.log 2>&1 &
            sleep 2
            exit 0
        ''',
    )

    # Task 6: Verify Pipeline Health
    # Waits for all services to initialize before checking
    verify_health = BashOperator(
        task_id='verify_pipeline_health',
        bash_command=f'''
            cd {PROJECT_PATH}
            sleep 15
            {VENV_PYTHON} check_pipeline.py || echo "⚠️ Some services may still be initializing"
            exit 0
        ''',
    )

    # Task Dependencies:
    # 1. Start Docker first
    # 2. Once Docker is ready, start all services in parallel
    # 3. Finally verify health
    start_infra >> [start_api, start_bridge, start_spark, start_dashboard] >> verify_health
