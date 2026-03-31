import os
from pathlib import Path
from typing import Any

import yaml


# Cache en memoria para evitar releer/parsing del YAML en cada llamada.
_REGISTRY_CACHE: dict[str, dict[str, Any]] | None = None


def _registry_path() -> Path:
    # Orden de prioridad:
    # 1) Ruta explicita por variable de entorno
    # 2) Registro en la raiz (compatibilidad)
    project_root = Path(__file__).resolve().parents[1]
    configured_root = os.getenv("SPARK_JOBS_REPO_ROOT", "").strip()

    candidate_paths: list[Path] = []
    if configured_root:
        candidate_paths.append(Path(configured_root) / "job_registry.yaml")

    candidate_paths.append(project_root / "job_registry.yaml")

    for candidate in candidate_paths:
        if candidate.exists():
            return candidate

    return candidate_paths[0]


def _load_registry() -> dict[str, dict[str, Any]]:
    global _REGISTRY_CACHE

    # Reusa el cache cuando ya se cargó el registro.
    if _REGISTRY_CACHE is not None:
        return _REGISTRY_CACHE

    with _registry_path().open("r", encoding="utf-8") as file:
        payload = yaml.safe_load(file) or {}

    jobs = payload.get("jobs")
    if not isinstance(jobs, dict):
        raise ValueError("job_registry.yaml must contain a top-level 'jobs' mapping.")

    # Guarda el mapping de jobs para futuras llamadas.
    _REGISTRY_CACHE = jobs
    return jobs


def get_job(job_name: str) -> dict[str, Any]:
    # API publica: devuelve la configuracion completa de un job por nombre.
    jobs = _load_registry()
    if job_name not in jobs:
        raise KeyError(f"Job '{job_name}' is not defined in job_registry.yaml")
    return jobs[job_name]
