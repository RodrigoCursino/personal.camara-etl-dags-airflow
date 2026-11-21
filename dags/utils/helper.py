from kubernetes.client import models as k8s
from uuid import uuid4, uuid5, NAMESPACE_URL
from datetime import datetime
from utils.logger import logger

def env(name, value):
    return k8s.V1EnvVar(name=name, value=value)


def secret_env(name, secret, key):
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)
        ),
    )


TASK_ENV_NAME = "TASK_ID"        # uuid por execução de task (muda a cada retry)
EXECUTION_ENV_NAME = "EXCUTION_ID"  # uuid por DAG run (igual para todas as tasks); nome conforme utils/variables.py

def ctx_data(context):
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
    if getattr(task, "arguments", None):
        data["arguments"] = task.arguments
    if getattr(task, "image", None):
        data["image"] = task.image
    if ti.start_date:
        data["duration_s"] = round((datetime.now(ti.start_date.tzinfo) - ti.start_date).total_seconds(), 1)
    return data

def set_trace_ids(context):
    """Injeta no env do pod: EXCUTION_ID (por DAG run) e TASK_ID (por execução de task)."""
    task = context["task"]
    # uuid5 determinístico: todas as tasks (e retries) do mesmo DAG run obtêm o mesmo valor
    execution_id = str(uuid5(NAMESPACE_URL, f"{context['dag'].dag_id}/{context['run_id']}"))
    task_id = str(uuid4())
    names = {EXECUTION_ENV_NAME, TASK_ENV_NAME}
    # Copia a lista para não mutar o env_vars compartilhado entre tasks
    task.env_vars = [e for e in (task.env_vars or []) if getattr(e, "name", None) not in names] + [
        k8s.V1EnvVar(name=EXECUTION_ENV_NAME, value=execution_id),
        k8s.V1EnvVar(name=TASK_ENV_NAME, value=task_id),
    ]
    logger.info(
        "ids de rastreio gerados",
        extra={"extra_data": {**ctx_data(context), "execution_id": execution_id, "task_uuid": task_id}},
    )

def log_start(context):
    logger.info("task iniciada", extra={"extra_data": ctx_data(context)})


def log_success(context):
    logger.info("task concluída", extra={"extra_data": ctx_data(context)})


def log_retry(context):
    data = ctx_data(context)
    ti = context["ti"]
    data["error"] = str(context.get("exception"))
    data["next_retry_in"] = str(context["task"].retry_delay)
    logger.warning(
        f"task falhou, nova tentativa {ti.try_number + 1}/{data['max_tries']}",
        extra={"extra_data": data},
    )

def log_failure(context):
    data = ctx_data(context)
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


