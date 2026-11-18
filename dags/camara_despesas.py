from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dags.utils.variables import env_vars
from utils.logger import logger


def _ctx_data(context):
    """Campos comuns a todos os logs de task."""
    ti = context["ti"]
    task = context["task"]
    data = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "try_number": ti.try_number,
        "max_tries": task.retries + 1,
        "logical_date": str(context.get("logical_date")),
    }
    # Parâmetros do pod (só os de negócio; env_vars nunca é logado, pode ter credencial)
    if getattr(task, "arguments", None):
        data["arguments"] = task.arguments
    if getattr(task, "image", None):
        data["image"] = task.image
    if ti.start_date:
        data["duration_s"] = round((datetime.now(ti.start_date.tzinfo) - ti.start_date).total_seconds(), 1)
    return data


def log_start(context):
    logger.info("task iniciada", extra={"extra_data": _ctx_data(context)})


def log_success(context):
    logger.info("task concluída", extra={"extra_data": _ctx_data(context)})


def log_retry(context):
    data = _ctx_data(context)
    ti = context["ti"]
    data["error"] = str(context.get("exception"))
    data["next_retry_in"] = str(context["task"].retry_delay)
    logger.warning(
        f"task falhou, nova tentativa {ti.try_number + 1}/{data['max_tries']}",
        extra={"extra_data": data},
    )


def log_failure(context):
    data = _ctx_data(context)
    data["error"] = str(context.get("exception"))
    logger.error("task falhou definitivamente (sem mais retries)", extra={"extra_data": data})


def _dag_run_data(context):
    dr = context["dag_run"]
    data = {"dag_id": dr.dag_id, "run_id": dr.run_id, "run_type": str(dr.run_type)}
    if dr.start_date:
        data["duration_s"] = round((datetime.now(dr.start_date.tzinfo) - dr.start_date).total_seconds(), 1)
    return data


def log_dag_success(context):
    logger.info("DAG run concluída com sucesso", extra={"extra_data": _dag_run_data(context)})


def log_dag_failure(context):
    data = _dag_run_data(context)
    data["reason"] = context.get("reason")
    logger.error("DAG run falhou", extra={"extra_data": data})


# Log de parse (1x por parse da DAG): ajuda a confirmar config carregada, sem expor valores.
logger.debug(
    "DAG camara_despesas carregada",
    extra={"extra_data": {"env_vars_keys": sorted(getattr(e, "name", str(e)) for e in env_vars)}},
)


@dag(
    dag_id="camara_despesas",
    start_date=datetime(2026, 11, 16),
    schedule="* 22 * * *",  # Roda todos os dias ás 22 horas
    catchup=False,
    max_active_runs=1,  # Evita runs simultâneas sobrecarregando o cluster
    default_args={
        "owner": "rcursino",
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
        "on_execute_callback": log_start,
        "on_success_callback": log_success,
        "on_retry_callback": log_retry,
        "on_failure_callback": log_failure,
    },
    on_success_callback=log_dag_success,
    on_failure_callback=log_dag_failure,
    tags=["etl", "camara", "kubernetes"],
)
def pipeline_camara():

    # Task Bronze usando o decorator @task.kubernetes
    @task.kubernetes(
        task_id="bronze",
        name="camara-bronze-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        arguments=["--env", "dev", "--domain", "despesas", "--layer", "bronze", "--uf", "SP", "--ano", "2026", "--mes", "5"],
        env_vars=env_vars,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        in_cluster=True,
        startup_timeout_seconds=120
    )
    def run_bronze():
        pass  # O corpo da função não é executado localmente; o Airflow injeta a lógica no Pod do K8s definido nos argumentos do decorator

    # Task Silver
    @task.kubernetes(
        task_id="silver",
        name="camara-silver-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        arguments=["--env", "dev", "--domain", "despesas", "--layer", "silver", "--uf", "SP", "--ano", "2026", "--mes", "5"],
        env_vars=env_vars,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        in_cluster=True,
        startup_timeout_seconds=120
    )
    def run_silver():
        pass

    # Task Gold
    @task.kubernetes(
        task_id="gold",
        name="camara-gold-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        arguments=["--env", "dev", "--domain", "despesas", "--layer", "gold", "--uf", "SP", "--ano", "2026", "--mes", "5"],
        env_vars=env_vars,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        in_cluster=True,
        startup_timeout_seconds=120
    )
    def run_gold():
        pass

    # Execução e encadeamento das tasks utilizando a TaskFlow API
    t_bronze = run_bronze()
    t_silver = run_silver()
    t_gold = run_gold()

    t_bronze >> t_silver >> t_gold

pipeline_camara = pipeline_camara()