# Dagster con Docker Compose

Este proyecto levanta un entorno local de Dagster con PostgreSQL usando Docker Compose.

## Estructura

```text
dagster-docker/
|- .env
|- docker-compose.yml
|- Dockerfile
|- requirements.txt
`- repo/
   `- repository.py
```

## Servicios

- `postgres`: base de datos PostgreSQL 14
- `dagster-daemon`: daemon de Dagster (schedules, sensores y runs)
- `dagster-webserver`: UI web de Dagster en el puerto `3000`

## Variables de entorno

El archivo `docker-compose.yml` toma los parametros de base de datos y Dagster desde `.env`.

Variables actuales:

```env
POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster
POSTGRES_DB=dagster

DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=dagster
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_HOST=postgres
DAGSTER_HOME=/opt/dagster/dagster_home
```

## Requisitos

- Docker Desktop instalado y ejecutandose
- Docker Compose v2 (`docker compose`)

## Levantar el entorno

Desde la carpeta `dagster-docker/` ejecuta:

```bash
docker compose up --build
```

Luego abre en el navegador:

`http://localhost:3000`

## Detener el entorno

```bash
docker compose down
```

## Job de ejemplo

El repositorio de Dagster esta en `repo/repository.py` y contiene el job `hello_job`:

- `hello` imprime `Hello Dagster!`
- `hello_job` ejecuta el op `hello`

## Notas

- El volumen `postgres_data` persiste los datos de PostgreSQL.
- El volumen `dagster_home` persiste el directorio `DAGSTER_HOME`.
