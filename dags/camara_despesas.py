from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from utils.variables import env_vars, UFS
from utils.logger import logger
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")

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


logger.debug(
    "DAG camara_despesas carregada",
    extra={"extra_data": {"env_vars_keys": sorted(getattr(e, "name", str(e)) for e in env_vars)}},
)


@dag(
    dag_id="camara_despesas",
    start_date=datetime(2026, 11, 16),
    schedule="0 22 * * *",
    catchup=False,
    max_active_runs=1,
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

    @task(task_id='generate_parameter_matrix')
    def build_parameter_matrix(**context) -> list[dict]:
        """
        Analisa o dag_run.conf enviado na execução ou define os defaults.
        Exemplos de conf aceitos:
        - {"partition": "202605", "ufs": ["SP"]}
        - {"from": "202601"}
        - {"list": ["202601", "202610"]}
        """
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        now = pendulum.now(local_tz)
        ufs = conf.get("ufs", UFS)
        
        periodos = []
        
        if "partition" in conf:
            val = conf["partition"]
            periodos.append({"ano": val[:4], "mes": val[4:]})

        elif "from" in conf:
            from_str = conf["from"]
            start_date = pendulum.datetime(int(from_str[:4]), int(from_str[4:]), 1, tz=local_tz)
            curr = start_date
            current_date = pendulum.datetime(now.year, now.month, 1, tz=local_tz)
            while curr <= current_date:
                periodos.append({"ano": str(curr.year), "mes": f"{curr.month:02d}"})
                curr = curr.add(months=1)

        elif "list" in conf:
            for item in conf["list"]:
                periodos.append({"ano": item[:4], "mes": item[4:]})
        else:
            # Default: Mês e ano atual se nenhum parâmetro de data for informado
            periodos.append({"ano": str(now.year), "mes": f"{now.month:02d}"})

        matrix = []
        for periodo in periodos:
            for uf in ufs:
                matrix.append({
                    "ano": periodo["ano"],
                    "mes": periodo["mes"],
                    "uf": uf
                })

        logger.info(f"Total de tarefas geradas para esta execução: {len(matrix)}")
        return matrix

    # --- Orquestração Dinâmica com .partial() e .expand() ---
    parametros_lista = build_parameter_matrix()

    # Função auxiliar para criar a definição do KubernetesPodOperator com .partial()
    def create_k8s_pod(layer: str):
        return KubernetesPodOperator(  # Ou KubernetesPodOperator
            task_id=f"etl_{layer}",
            name=f"camara-{layer}-worker",
            namespace="etl",
            image="rcursino/camara-etl:v1",
            cmds=["python", "-m", "src.main"],
            arguments=[
                "--env", "dev",
                "--domain", "despesas",
                "--layer", layer,
                "--uf", "{{ task_instance.xcom_pull(task_ids='generate_parameter_matrix')[ti.map_index]['uf'] }}",
                "--ano", "{{ task_instance.xcom_pull(task_ids='generate_parameter_matrix')[ti.map_index]['ano'] }}",
                "--mes", "{{ task_instance.xcom_pull(task_ids='generate_parameter_matrix')[ti.map_index]['mes'] }}"
            ],
            env_vars=env_vars,
            get_logs=True,
            on_finish_action="delete_succeeded_pod",
            in_cluster=True,
            startup_timeout_seconds=120,
        )
    
    bronze = KubernetesPodOperator.partial(
        task_id="bronze",
        name="camara-bronze-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        env_vars=env_vars,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        in_cluster=True,
        startup_timeout_seconds=120,
    ).expand(arguments=parametros_lista.map(lambda item: [
        "--env", "dev",
        "--domain", "despesas",
        "--layer", "bronze",
        "--uf", item["uf"],
        "--ano", item["ano"],
        "--mes", item["mes"]
    ]))

    silver = KubernetesPodOperator.partial(
        task_id="silver",
        name="camara-silver-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        env_vars=env_vars,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        in_cluster=True,
        startup_timeout_seconds=120,
    ).expand(arguments=parametros_lista.map(lambda item: [
        "--env", "dev",
        "--domain", "despesas",
        "--layer", "silver",
        "--uf", item["uf"],
        "--ano", item["ano"],
        "--mes", item["mes"]
    ]))

    gold = KubernetesPodOperator.partial(
        task_id="gold",
        name="camara-gold-worker",
        namespace="etl",
        image="rcursino/camara-etl:v1",
        cmds=["python", "-m", "src.main"],
        env_vars=env_vars,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        in_cluster=True,
        startup_timeout_seconds=120,
    ).expand(arguments=parametros_lista.map(lambda item: [
        "--env", "dev",
        "--domain", "despesas",
        "--layer", "gold",
        "--uf", item["uf"],
        "--ano", item["ano"],
        "--mes", item["mes"]
    ]))

    # Encadeamento das camadas mapeadas em paralelo
    bronze >> silver >> gold

pipeline_camara()