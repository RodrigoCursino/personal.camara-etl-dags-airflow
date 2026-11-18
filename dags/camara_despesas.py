from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

def env(name, value):
    return k8s.V1EnvVar(name=name, value=value)


def secret_env(name, secret, key):
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)
        ),
    )

env_vars = [
    # Banco de dados
    env("DATABASE_TYPE", "duckdb"),
    env("DATABASE_PATH", "data/camara_warehouse.dev.db"),
    # Ambiente
    env("ENVIRONMENT_NAME", "dev"),
    env("DEBUG", "true"),
    env("LOGGING_LEVEL", "DEBUG"),
    # Fila (RabbitMQ)
    env("QUEUE_TYPE", "rabbitmq"),
    env("RABBITMQ_HOST", "rabbitmq"),
    env("RABBITMQ_PORT", "5672"),
    env("RABBITMQ_USER", "guest"),
    env("RABBITMQ_PASSWORD", "guest"),
    # API da Câmara
    env("CAMARA_API_BASE_URL", "https://dadosabertos.camara.leg.br/api"),
    env("CAMARA_API_VERSION", "v2"),
    env("CAMARA_API_TIMEOUT", "15"),
    env("CAMARA_API_MAX_RETRIES", "5"),
    # Engine de consultas
    env("QUERIES_ENGINE", "duckdb"),
    # Storage (MinIO via S3)
    env("STORAGE_TYPE", "minio"),
    env("S3_BUCKET", "camara-warehouse"),
    env("S3_ENDPOINT", "http://minio-service.minio.svc.cluster.local:9000"),
    secret_env("S3_ACCESS_KEY", "minio-credentials", "accesskey"),
    secret_env("S3_SECRET_KEY", "minio-credentials", "secretkey"),
]



with DAG(
    dag_id="camara_despesas",
    start_date=datetime(2026, 11, 16),
    schedule="30 * * * *",  # Roda de hora em hora, no minuto 30
    catchup=False,
    max_active_runs=1,  # Evita runs simultâneas sobrecarregando o cluster
    default_args={
        "owner": "rcursino",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["etl", "camara", "kubernetes"],
) as dag:

    bronze = KubernetesPodOperator(
        task_id="bronze",
        name="camara-bronze-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        arguments=["--env", "dev", "--domain", "despesas", "--layer", "bronze", "--uf", "SP", "--ano", "2026", "--mes", "5"],
        env_vars=env_vars,
        get_logs=True,                           # Joga os logs do Python direto para a UI do Airflow
        on_finish_action="delete_succeeded_pod", 
        in_cluster=True,                         # Comunicação interna segura com a API do K8s
        startup_timeout_seconds=120
    )

    silver = KubernetesPodOperator(
        task_id="silver",
        name="camara-silver-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        arguments=["--env", "dev", "--domain", "despesas", "--layer", "silver", "--uf", "SP", "--ano", "2026", "--mes", "5"],
        env_vars=env_vars,
        get_logs=True,                           # Joga os logs do Python direto para a UI do Airflow
        on_finish_action="delete_succeeded_pod", 
        in_cluster=True,                         # Comunicação interna segura com a API do K8s
        startup_timeout_seconds=120
    )

    gold = KubernetesPodOperator(
        task_id="gold",
        name="camara-gold-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        arguments=["--env", "dev", "--domain", "despesas", "--layer", "gold", "--uf", "SP", "--ano", "2026", "--mes", "5"],
        env_vars=env_vars,
        get_logs=True,                           # Joga os logs do Python direto para a UI do Airflow
        on_finish_action="delete_succeeded_pod", 
        in_cluster=True,                         # Comunicação interna segura com a API do K8s
        startup_timeout_seconds=120
    )

    bronze >> silver >> gold

