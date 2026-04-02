import importlib.util
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

try:
    from .job_registry import get_job
except ImportError:  # pragma: no cover - fallback for direct module execution
    from job_registry import get_job


def _parse_spark_conf(raw_conf: str) -> dict[str, str]:
    # Convierte "k=v;k2=v2" en dict para pasarlo luego como --conf.
    conf_pairs: dict[str, str] = {}
    for chunk in raw_conf.split(";"):
        item = chunk.strip()
        if not item:
            continue

        if "=" not in item:
            raise ValueError(
                "Invalid SPARK_SUBMIT_CONF entry. Expected format key=value;key2=value2"
            )

        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ValueError(
                "Invalid SPARK_SUBMIT_CONF entry. Key and value are required."
            )

        conf_pairs[key] = value

    return conf_pairs


def _apply_job_spark_conf(conf_by_key: dict[str, str], spark_conf: dict[str, Any]) -> None:
    # Mapea claves amigables del registry a claves Spark estandar.
    if "executor_instances" in spark_conf:
        conf_by_key["spark.executor.instances"] = str(spark_conf["executor_instances"])
    if "executor_memory" in spark_conf:
        conf_by_key["spark.executor.memory"] = str(spark_conf["executor_memory"])
    if "driver_memory" in spark_conf:
        conf_by_key["spark.driver.memory"] = str(spark_conf["driver_memory"])

    extra_conf = spark_conf.get("conf", {})
    if not isinstance(extra_conf, dict):
        raise ValueError("'spark.conf' must be a dictionary in job_registry.yaml")

    # Permite sobreescrituras finas por job (spark.conf.<key>). 
    for key, value in extra_conf.items():
        conf_by_key[str(key)] = str(value)


def _build_job_argument_list(job_name: str, params: dict[str, Any] | None) -> list[str]:
    # Solo permite parametros declarados en el registry para evitar entradas invalidas.
    if not params:
        return []

    job = get_job(job_name)
    allowed_params = job.get("params", [])
    if not isinstance(allowed_params, list):
        raise ValueError(f"Job '{job_name}' has invalid params definition in job_registry.yaml")

    args: list[str] = []
    for key, value in params.items():
        if key not in allowed_params:
            raise ValueError(f"Parameter '{key}' is not allowed for job '{job_name}'")
        args.extend([f"--{key}", str(value)])

    return args


def _build_spark_submit_parts(job_name: str) -> tuple[list[str], str]:
    # Arma la base de spark-submit con conf global + conf especifica del job.
    job = get_job(job_name)
    entrypoint = str(job["entrypoint"])

    spark_master_url = os.getenv("SPARK_MASTER_URL", "").strip()
    spark_deploy_mode = os.getenv("SPARK_DEPLOY_MODE", "").strip()
    spark_jars = os.getenv("SPARK_JARS", "").strip()
    spark_packages = os.getenv("SPARK_PACKAGES", "").strip()
    spark_submit_conf = os.getenv("SPARK_SUBMIT_CONF", "").strip()

    conf_by_key = _parse_spark_conf(spark_submit_conf) if spark_submit_conf else {}

    spark_conf = job.get("spark", {})
    if not isinstance(spark_conf, dict):
        raise ValueError(f"Job '{job_name}' has invalid spark definition in job_registry.yaml")
    _apply_job_spark_conf(conf_by_key, spark_conf)

    if spark_deploy_mode.lower() == "client":
        # En client mode el driver debe ser alcanzable por executors remotos.
        driver_host = os.getenv("SPARK_DRIVER_HOST", "").strip()
        driver_bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0").strip()

        if "spark.driver.host" not in conf_by_key:
            if not driver_host:
                raise ValueError(
                    "SPARK_DRIVER_HOST is required when SPARK_DEPLOY_MODE=client "
                    "(or define spark.driver.host in SPARK_SUBMIT_CONF or job spark.conf)."
                )
            conf_by_key["spark.driver.host"] = driver_host

        if "spark.driver.bindAddress" not in conf_by_key and driver_bind_address:
            conf_by_key["spark.driver.bindAddress"] = driver_bind_address

    command = ["spark-submit"]

    if spark_master_url:
        command.extend(["--master", spark_master_url])

    if spark_deploy_mode:
        command.extend(["--deploy-mode", spark_deploy_mode])

    if spark_jars:
        command.extend(["--jars", spark_jars])
    elif spark_packages:
        command.extend(["--packages", spark_packages])

    for key, value in conf_by_key.items():
        command.extend(["--conf", f"{key}={value}"])

    return command, entrypoint


def _resolve_local_entrypoint(project_root: Path, entrypoint: str) -> Path:
    # Prioriza ruta externa configurada y mantiene fallback al layout legacy.
    configured_root = os.getenv("SPARK_JOBS_REPO_ROOT", "").strip()

    candidate_paths: list[Path] = []
    if configured_root:
        candidate_paths.append(Path(configured_root) / entrypoint)

    candidate_paths.append(project_root / entrypoint)

    for candidate in candidate_paths:
        if candidate.exists():
            return candidate

    # Fallback: resuelve a traves del paquete etl_jobs instalado.
    module_name = "etl_jobs." + entrypoint.removesuffix(".py").replace("/", ".")
    spec = importlib.util.find_spec(module_name)
    if spec and spec.origin:
        return Path(spec.origin)

    return candidate_paths[0]


def _resolve_local_cwd(project_root: Path, entrypoint_path: Path) -> Path:
    configured_root = os.getenv("SPARK_JOBS_REPO_ROOT", "").strip()

    if configured_root:
        candidate_root = Path(configured_root)
        if candidate_root.exists() and entrypoint_path.is_relative_to(candidate_root):
            return candidate_root

    return project_root


def run_spark_job(job_name: str, params: dict[str, Any] | None = None) -> str:
    # Punto de entrada principal usado por Dagster ops.
    project_root = Path(__file__).resolve().parents[1]
    command_prefix, entrypoint = _build_spark_submit_parts(job_name)
    job_args = _build_job_argument_list(job_name, params)

    run_mode = os.getenv("SPARK_RUNNER_MODE", "ssh").strip().lower()

    if run_mode == "ssh":
        # Modo remoto: construye y ejecuta spark-submit via SSH.
        ssh_host = os.getenv("SPARK_SSH_HOST", "").strip()
        ssh_user = os.getenv("SPARK_SSH_USER", "").strip()
        ssh_port = os.getenv("SPARK_SSH_PORT", "22").strip()
        ssh_key = os.getenv("SPARK_SSH_IDENTITY_FILE", "").strip()
        remote_jobs_root = os.getenv("SPARK_REMOTE_JOBS_ROOT", "/opt/spark-jobs").rstrip("/")

        if not ssh_host:
            raise ValueError("SPARK_SSH_HOST is required when SPARK_RUNNER_MODE=ssh")

        destination = f"{ssh_user}@{ssh_host}" if ssh_user else ssh_host
        remote_entrypoint = f"{remote_jobs_root}/{entrypoint}"
        remote_submit = " ".join(
            shlex.quote(part) for part in [*command_prefix, remote_entrypoint, *job_args]
        )

        ssh_command = ["ssh"]
        if ssh_key:
            ssh_command.extend(["-i", ssh_key])
        if ssh_port:
            ssh_command.extend(["-p", ssh_port])
        ssh_command.extend([destination, remote_submit])

        result = subprocess.run(
            ssh_command,
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root),
        )
    else:
        # Modo local: ejecuta spark-submit dentro del mismo contenedor/host de Dagster.
        local_entrypoint = _resolve_local_entrypoint(project_root, entrypoint)
        if not local_entrypoint.exists():
            raise FileNotFoundError(f"Job entrypoint not found: {local_entrypoint}")

        local_cwd = _resolve_local_cwd(project_root, local_entrypoint)

        result = subprocess.run(
            [*command_prefix, str(local_entrypoint), *job_args],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(local_cwd),
        )

    if result.returncode != 0:
        # Propaga stderr/stdout para que Dagster registre causa real del fallo.
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        if stderr and stdout:
            details = f"STDERR:\n{stderr}\n\nSTDOUT:\n{stdout}"
        else:
            details = stderr if stderr else stdout
        raise RuntimeError(f"Error running job '{job_name}':\n{details}")

    return result.stdout
