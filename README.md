# Dagster + Spark ETL (InfluxDB y CryptoCompare -> Cloudflare R2)

Este proyecto levanta un entorno local de Dagster con PostgreSQL y ejecuta ETLs con PySpark:

- `InfluxDB -> Spark transform -> Parquet particionado -> Cloudflare R2`
- `CryptoCompare (BTC/USD) -> Spark transform -> Bronze/Silver Parquet -> Cloudflare R2`

Dagster se encarga de la orquestacion (dependencies, retries, schedules) y lanza el script de Spark con `spark-submit`.

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
`- spark_jobs/
   `- influx_to_r2.py
   `- cryptocompare_to_r2.py
```

## Jobs y schedule

Archivo: `repo/repository.py`

- `hello_job`: job basico de prueba.
- `influx_r2_etl_job`: lanza `spark-submit spark_jobs/influx_to_r2.py`.
- `cryptocompare_r2_etl_job`: lanza `spark-submit spark_jobs/cryptocompare_to_r2.py`.
- `hourly_influx_r2_schedule`: cron `0 * * * *` (cada hora, UTC).
- `daily_cryptocompare_r2_schedule`: cron `0 0 * * *` (diario, UTC).

El op de Spark tiene `RetryPolicy(max_retries=3, delay=30)`.

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

# InfluxDB 1.x
INFLUXDB_HOST=localhost
INFLUXDB_PORT=8086
INFLUXDB_DATABASE=homeassistant
INFLUXDB_USERNAME=admin
INFLUXDB_PASSWORD=password
# Opcional: ventana de tiempo para la query por defecto (si INFLUXDB_QUERY esta vacia)
# Formato: <numero><unidad> ; unidades soportadas: ms, s, m, h, d, w
# Ejemplos: 1h, 12h, 7d, 30d
INFLUXDB_LOOKBACK=1d
# Opcional: query InfluxQL completa (tiene prioridad sobre INFLUXDB_LOOKBACK)
INFLUXDB_QUERY=
SPARK_PACKAGES=org.apache.hadoop:hadoop-aws:3.4.2

# Cloudflare R2
R2_ENDPOINT=https://<account_id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=replace_me
R2_SECRET_ACCESS_KEY=replace_me
R2_BUCKET=replace_me
R2_PREFIX=influx/data
R2_REGION=auto

# CryptoCompare -> R2 (usa el mismo bucket R2_BUCKET)
# Destinos fijos:
#   s3a://<R2_BUCKET>/btc-lakehouse-bucket/bronze/
#   s3a://<R2_BUCKET>/btc-lakehouse-bucket/silver/
```

Notas para R2:

- `R2_ENDPOINT` debe incluir `https://` (ejemplo: `https://<account_id>.r2.cloudflarestorage.com`).
- Se usa `s3a` con `path.style.access=true` y SSL habilitado.
- `SPARK_PACKAGES` debe incluir `org.apache.hadoop:hadoop-aws:3.4.2` para habilitar `S3AFileSystem`.

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

## Transformaciones aplicadas

En `spark_jobs/influx_to_r2.py`:

- Lee datos desde InfluxDB 1.x via `influxdb` (InfluxQL) y los convierte a DataFrame de Spark.
- Si `INFLUXDB_QUERY` esta vacia, construye la query por defecto usando `INFLUXDB_LOOKBACK` (default: `1d`).
- Limpia columnas tecnicas cuando existen: `_start`, `_stop`, `result`, `table`.
- Renombra `_time` a `timestamp`.
- Agrega `date = to_date(timestamp)` para particion.

Escritura final:

- Formato: Parquet
- Modo: `overwrite` (dinamico por particion `date`)
- Destino: `s3a://<R2_BUCKET>/<R2_PREFIX>/`
- Particion: `partitionBy("date")` (estructura esperada: `date=YYYY-MM-DD/`)
- Limpieza automatica de directorios temporales `.spark-staging-*`

En `spark_jobs/cryptocompare_to_r2.py`:

- Descarga historico diario BTC/USD desde CryptoCompare (`histoday`, `allData=true`).
- Define esquema explicito para forzar tipos numericos (`DoubleType`, `LongType`).
- Construye:
   - `bronze`: datos crudos + columna `fecha`.
   - `silver`: `fecha`, `precio_cierre`, `maximo`, `minimo`, `volumen_btc`.
- Escritura final (ambas capas) en la misma cubeta configurada por `R2_BUCKET`:
   - `s3a://<R2_BUCKET>/btc-lakehouse-bucket/bronze/`
   - `s3a://<R2_BUCKET>/btc-lakehouse-bucket/silver/`
- Modo `overwrite` dinamico por particion `fecha` + limpieza de `.spark-staging-*`.

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

## Dependencias principales

- `dagster`, `dagster-webserver`, `dagster-postgres`
- `pyspark`
- `influxdb`
- `pandas`
- `pyarrow`
- `requests`

## Notas

- `Dockerfile` instala Java (`openjdk-17-jre-headless`) porque Spark lo requiere.
- Volumen `postgres_data`: persiste PostgreSQL.
- Volumen `dagster_home`: persiste `DAGSTER_HOME`.
