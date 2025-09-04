import pytest
from pathlib import Path
from unittest.mock import patch
from pathlib import Path
from src.modulos.ingesta import iter_csv_files, load_running_stats_from_db, persist_running_stats
from src.modulos.stats import RunningStats

from typing import Dict, Any


# -----------------------------
# Fixture para crear un directorio temporal! con archivos que se le idiquen (CSV en este caso)
# -----------------------------
@pytest.fixture
def csv_config(tmp_path):
    # Crear archivos CSV de ejemplo
    archivos = ["2012-1.csv", "2012-2.csv", "2012-3.csv", "validacion.csv"]
    for archivo in archivos:
        (tmp_path / archivo).write_text("col1,col2\n1,2\n3,4")
    
    # Config simulando archivo config.yaml
    config: Dict[str, Any] = {
        "CSV": {
            "CSV_DIR": str(tmp_path),
            "file_validation": "validacion.csv"
        }
    }
    return config, archivos

# -----------------------------
#  test de iter_csv_files
# -----------------------------
def test_iter_csv_files_include_validation(csv_config):
    config, archivos = csv_config
    # Si se envia include_validation=True, este debe incluir validacion.csv!
    result = list(iter_csv_files(config, include_validation=True))
    expected_files = sorted([Path(config["CSV"]["CSV_DIR"]) / f for f in archivos])
    assert result == expected_files

def test_iter_csv_files_exclude_validation(csv_config):
    config, archivos = csv_config
    # Se no se envia include_validation=True, este debe excluir validacion.csv
    result = list(iter_csv_files(config, include_validation=False))
    expected_files = sorted([Path(config["CSV"]["CSV_DIR"]) / f for f in archivos if f != "validacion.csv"])
    assert result == expected_files

# ----------------------------
# Test de load_running_stats_from_db
# ----------------------------
@patch("src.modulos.ingesta.get_running_stats")
def test_load_running_stats_from_db(mock_get_rs):
    mock_get_rs.return_value = {"count": 10, "mean": 5.0, "min": 1.0, "max": 9.0}
    config = {"dummy": "config"}
    
    rs = load_running_stats_from_db(config)
    
    assert isinstance(rs, RunningStats)
    assert rs.count == 10
    assert rs.mean == 5.0
    assert rs.min == 1.0
    assert rs.max == 9.0
    mock_get_rs.assert_called_once_with(config)

# ----------------------------
# Test de persist_running_stats
# ----------------------------
@patch("src.modulos.ingesta.update_running_stats")
def test_persist_running_stats(mock_update_rs):
    rs = RunningStats(count=3, mean=2.0, min=1.0, max=3.0)
    config = {"dummy": "config"}
    
    persist_running_stats(rs, config)
    
    mock_update_rs.assert_called_once_with(3, 2.0, 1.0, 3.0, config)
