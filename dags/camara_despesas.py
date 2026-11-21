from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from utils.variables import env_vars, UFS, ETL_RESOURCES, MAX_PARALLEL_PODS, local_tz, IMAGE
from utils.logger import logger
from utils.helper import set_trace_ids, log_dag_failure, log_retry, log_success, log_start, log_failure, log_dag_success
import pendulum

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
        "pre_execute": set_trace_ids,
        "on_execute_callback": log_start,
        "on_success_callback": log_success,
        "on_retry_callback": log_retry,
        "on_failure_callback": log_failure,
    },
    on_success_callback=log_dag_success,
    on_failure_callback=log_dag_failure,
    tags=["etl", "camara", "despesas", "kubernetes"],
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

    def etl_layer(layer: str):
        """Uma instância mapeada (1 pod) por item da matriz UF x período."""

        return KubernetesPodOperator.partial(
            task_id=layer,
            name=f"camara-{layer}-worker",
            namespace="etl",
            image=IMAGE,
            cmds=["python", "-m", "src.main"],
            env_vars=env_vars,
            container_resources=ETL_RESOURCES,
            get_logs=True,
            on_finish_action="delete_succeeded_pod",
            in_cluster=True,
            startup_timeout_seconds=120,
            max_active_tis_per_dag=MAX_PARALLEL_PODS,
        ).expand(
            arguments=parametros_lista.map(
                lambda item: [
                    "--env", "dev",
                    "--domain", "despesas",
                    "--layer", layer,
                    "--uf", item["uf"],
                    "--ano", item["ano"],
                    "--mes", item["mes"],
                ]
            )
        )

    bronze = etl_layer("bronze")
    silver = etl_layer("silver")
    gold = etl_layer("gold")

    # Cada camada só começa quando a anterior terminou para todas as UFs
    bronze >> silver >> gold


pipeline_camara()
