# Dagster + Spark ETL (InfluxDB y CryptoCompare -> Cloudflare R2)

Este proyecto levanta un entorno local de Dagster con PostgreSQL y ejecuta ETLs con PySpark:

- `InfluxDB -> Spark transform -> Parquet particionado -> Cloudflare R2`
- `CryptoCompare (BTC/USD) -> Spark transform -> Bronze/Silver Parquet -> Cloudflare R2`

Dagster se encarga solo de la orquestacion (dependencies, retries, schedules) y delega la ejecucion a un runner generico basado en registro (`job_registry.yaml`).

Los jobs Spark viven fuera de este repositorio, en un repositorio independiente.

## Arquitectura

```text
InfluxDB / CryptoCompare API
   |
   | (query / HTTP)
   v
Spark Jobs (PySpark)
   |
   | transformaciones
   v
Parquet particionado por fecha
   |
   | write (s3a)
   v
Cloudflare R2
   |
   v
Dagster (orquestador)
```

## Estructura

```text
.
|- .env
|- docker-compose.yml
|- Dockerfile
|- requirements.txt
|- repo/
|  `- repository.py
|  `- job_registry.py
|  `- spark_runner.py
|- job_registry.yaml
`- dagster.yaml
```

## Jobs y schedule

Archivo: `repo/repository.py`

- `hello_job`: job basico de prueba.
- `influx_r2_etl_job`: ejecuta `run_spark_job("influx_to_r2")`.
- `cryptocompare_r2_etl_job`: ejecuta `run_spark_job("cryptocompare_to_r2")`.
- `hourly_influx_r2_schedule`: cron `0 * * * *` (cada hora, UTC).
- `daily_cryptocompare_r2_schedule`: cron `0 0 * * *` (diario, UTC).

El op de Spark tiene `RetryPolicy(max_retries=3, delay=30)`.

## Registro de jobs

Archivo: `job_registry.yaml`

Cada job declara:

- `entrypoint`: script real a ejecutar.
- `description`: descripcion funcional.
- `spark`: recursos y conf especifica por job.
- `params`: parametros permitidos para ejecucion.

El loader esta en `repo/job_registry.py` y el runner en `repo/spark_runner.py`.

## Variables de entorno

Define estas variables en `.env`:

```env
# Postgres / Dagster
POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster
POSTGRES_DB=dagster

DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=dagster
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_HOST=postgres
DAGSTER_HOME=/opt/dagster/dagster_home

# Runner de ejecucion desacoplado (default: ssh)
SPARK_RUNNER_MODE=ssh
SPARK_SSH_HOST=
SPARK_SSH_USER=
SPARK_SSH_PORT=22
SPARK_SSH_IDENTITY_FILE=
SPARK_REMOTE_JOBS_ROOT=/opt/spark-jobs
SPARK_JOBS_REPO_ROOT=
```

Las variables de conexion de jobs (InfluxDB, R2 y Spark runtime) ahora viven en:

- el repositorio externo de jobs Spark

Con este esquema, Dagster no necesita credenciales de fuentes/destinos para iniciar.

## Configuracion de instancia Dagster

Este proyecto incluye `dagster.yaml` en la raiz para forzar que Dagster use PostgreSQL como storage de runs/event logs/schedules.

- Evita fallback a SQLite en `DAGSTER_HOME`.
- Previene errores de migracion como `alembic_version has more than one head`.

`Dockerfile` copia `dagster.yaml` dentro de la imagen en:

`/opt/dagster/app/dagster.yaml`

Al iniciar, cada servicio de Dagster sobrescribe ese archivo en:

`/opt/dagster/dagster_home/dagster.yaml`

Esto evita el error en servidores donde un bind mount ausente puede terminar creando `dagster.yaml` como directorio.
Tambien corrige casos donde `dagster.yaml` en el volumen queda vacio (0 bytes).

### Si ya fallaba en servidor con `IsADirectoryError`

Si ya existe un estado previo corrupto en el volumen (`dagster.yaml` como carpeta o vacio), recrea solo el volumen `dagster_home`:

```bash
docker compose down
docker volume rm dagster_dagster_home
docker compose up -d --build
```

## Jobs Spark

Las transformaciones y credenciales de conectividad se gestionan en el repositorio externo de jobs Spark.
Este repositorio solo conserva la orquestacion Dagster y el runner para invocacion remota/local.

## Levantar el entorno

Desde la raiz del proyecto:

```bash
docker compose up --build
```

UI de Dagster:

`http://localhost:3000`

UI de Dagster (solo lectura):

`http://localhost:3001`

## UI admin vs UI viewer (solo lectura)

En `docker-compose.yml` hay dos webservers:

- `dagster-webserver` (admin) en `3000`: permite lanzar runs, materializaciones y cambiar estado de schedules/sensors.
- `dagster-webserver-readonly` (viewer) en `3001`: inicia con `--read-only` y bloquea mutaciones desde la UI.

Esto cubre el caso de "version alterna" de la UI para observacion. Si se necesita control real por usuario (RBAC), Dagster OSS no trae gestion de usuarios/permisos fina por defecto; normalmente se resuelve con un proxy de autenticacion/autorizacion o con Dagster+.

## Detener el entorno

```bash
docker compose down
```

### Ejecucion local vs cluster remoto

La configuracion de Spark para ejecucion de jobs se administra en el repositorio externo
y en el runtime donde se ejecutan esos jobs.

## Dependencias principales

- `dagster`, `dagster-webserver`, `dagster-postgres`
- `PyYAML`

## Notas

- Dependencias de Spark y conectores de datos quedan en el repositorio `spark-jobs/`.
- Volumen `postgres_data`: persiste PostgreSQL.
- Volumen `dagster_home`: persiste `DAGSTER_HOME`.
