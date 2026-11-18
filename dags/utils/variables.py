from utils.helper import env, secret_env

UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", 
"MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", 
"PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]


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