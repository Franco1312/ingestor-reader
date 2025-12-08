"""Servidor web para exponer el ETL pipeline como servicio HTTP."""

import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file FIRST
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

from flask import Flask, jsonify, request
from src.cli import run_etl
from src.infrastructure.config_loader import YamlConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Suppress noisy third-party logs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)

app = Flask(__name__)
logger = logging.getLogger(__name__)


def _get_available_datasets():
    """Obtiene la lista de datasets disponibles.
    
    Returns:
        Lista de nombres de datasets disponibles.
    """
    config_dir = Path("config/datasets")
    datasets = []
    
    if config_dir.exists():
        for file_path in config_dir.glob("*.yml"):
            dataset_id = file_path.stem
            datasets.append(dataset_id)
        for file_path in config_dir.glob("*.yaml"):
            dataset_id = file_path.stem
            if dataset_id not in datasets:
                datasets.append(dataset_id)
    
    return sorted(datasets)


@app.route("/health", methods=["GET"])
def health_check():
    """Endpoint de health check."""
    return jsonify({"status": "healthy", "service": "ingestor-reader-v3"}), 200


@app.route("/api/v1/datasets", methods=["GET"])
def list_datasets():
    """Lista todos los datasets disponibles.
    
    Returns:
        JSON con la lista de datasets disponibles.
    """
    try:
        datasets = _get_available_datasets()
        return jsonify({
            "status": "success",
            "datasets": datasets,
            "count": len(datasets),
        }), 200
    except Exception as e:
        logger.exception(f"Error listando datasets: {e}")
        return jsonify({
            "status": "error",
            "message": f"Error interno: {str(e)}",
        }), 500


@app.route("/api/v1/etl/<dataset_id>", methods=["POST"])
def run_etl_endpoint(dataset_id: str):
    """Ejecuta el ETL pipeline para un dataset específico.
    
    Args:
        dataset_id: Identificador del dataset a procesar.
    
    Returns:
        JSON con el resultado de la ejecución.
    """
    try:
        logger.info(f"Ejecutando ETL para dataset: {dataset_id}")
        exit_code = run_etl(dataset_id)
        
        if exit_code == 0:
            return jsonify({
                "status": "success",
                "message": f"ETL completado exitosamente para dataset: {dataset_id}",
                "dataset_id": dataset_id,
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": f"ETL falló para dataset: {dataset_id}",
                "dataset_id": dataset_id,
                "exit_code": exit_code,
            }), 500
    
    except Exception as e:
        logger.exception(f"Error ejecutando ETL para dataset {dataset_id}: {e}")
        return jsonify({
            "status": "error",
            "message": f"Error interno: {str(e)}",
            "dataset_id": dataset_id,
        }), 500


@app.route("/api/v1/etl", methods=["POST"])
def run_etl_from_body():
    """Ejecuta el ETL pipeline con dataset_id desde el body del request.
    
    Body esperado:
        {
            "dataset_id": "bcra_infomondia_series"
        }
    
    Returns:
        JSON con el resultado de la ejecución.
    """
    try:
        data = request.get_json()
        
        if not data or "dataset_id" not in data:
            return jsonify({
                "status": "error",
                "message": "dataset_id es requerido en el body del request",
            }), 400
        
        dataset_id = data["dataset_id"]
        logger.info(f"Ejecutando ETL para dataset: {dataset_id}")
        exit_code = run_etl(dataset_id)
        
        if exit_code == 0:
            return jsonify({
                "status": "success",
                "message": f"ETL completado exitosamente para dataset: {dataset_id}",
                "dataset_id": dataset_id,
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": f"ETL falló para dataset: {dataset_id}",
                "dataset_id": dataset_id,
                "exit_code": exit_code,
            }), 500
    
    except Exception as e:
        logger.exception(f"Error ejecutando ETL: {e}")
        return jsonify({
            "status": "error",
            "message": f"Error interno: {str(e)}",
        }), 500




if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

