import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

SENSITIVE_FIELDS = {"password", "token", "secret", "authorization"}

_ENV_LOG_LEVELS = {
    "dev": logging.DEBUG,
    "staging": logging.INFO,
    "prod": logging.INFO,
}


def mask_sensitive(data: dict | None):
    if not isinstance(data, dict):
        return data
    return {k: "***" if k.lower() in SENSITIVE_FIELDS else v for k, v in data.items()}


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "extra_data"):
            log["data"] = mask_sensitive(record.extra_data)

        if record.exc_info:
            log["exception"] = {
                "type": record.exc_info[0].__name__,
                "msg": str(record.exc_info[1]),
                "stack": traceback.format_exception(*record.exc_info),
            }

        return json.dumps(log, ensure_ascii=False, default=str)


def setup_logger(env: str | None = None, name: str = "camara_dag") -> logging.Logger:
    """Logger JSON próprio da DAG.

    Diferente do ETL, NÃO mexe no root logger nem no stdout: o Airflow
    gerencia os handlers dele (task log, scheduler, etc.).
    """
    env = env or os.getenv("ENVIRONMENT_NAME", "dev")
    log = logging.getLogger(name)
    log.setLevel(_ENV_LOG_LEVELS.get(env, logging.DEBUG))
    log.propagate = False

    if not any(isinstance(h.formatter, JsonFormatter) for h in log.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        log.addHandler(handler)

    return log


logger = setup_logger()
