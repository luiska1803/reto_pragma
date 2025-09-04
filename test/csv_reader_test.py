import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from pathlib import Path
from src.submodulos.csv_reader import CSVReader

@pytest.fixture
def csv_config(tmp_path):
    # Crear archivo CSV temporal
    file_path = tmp_path / "test.csv"
    file_path.write_text("col1,col2\n1,2\n3,4")
    
    config = {
        "CSV": {
            "separadores": ",",
            "usecols": ["col1", "col2"],
            "usar_chunk": True
        }
    }
    return config, str(file_path)

# -----------------------------
# Test init
# -----------------------------
def test_init(csv_config):
    config, _ = csv_config
    reader = CSVReader(config)
    assert reader.config == config["CSV"]

# -----------------------------
# Test error si no se pasa el 'file_path'
# -----------------------------
def test_run_no_file_path(csv_config):
    config, _ = csv_config
    reader = CSVReader(config)
    with pytest.raises(ValueError, match="CSVReader requiere un 'file_path'"):
        reader.run(file_path=None)

# -----------------------------
# Test error si el archivo no existe
# -----------------------------
def test_run_file_not_found(csv_config):
    config, _ = csv_config
    reader = CSVReader(config)
    with pytest.raises(FileNotFoundError):
        reader.run(file_path="no_existe.csv")

# -----------------------------
# Test error para un chunksize inválido
# -----------------------------
def test_run_invalid_chunksize(csv_config):
    config, file_path = csv_config
    reader = CSVReader(config)
    with pytest.raises(ValueError):
        reader.run(file_path=file_path, chunksize=0)
    with pytest.raises(ValueError):
        reader.run(file_path=file_path, chunksize=1.5)

# -----------------------------
# Test lectura de archivo CSV con chunksize
# -----------------------------
@patch("pandas.read_csv")
def test_run_with_chunksize(mock_read_csv, csv_config):
    config, file_path = csv_config
    reader = CSVReader(config)
    mock_df = MagicMock()
    mock_read_csv.return_value = mock_df

    result = reader.run(file_path=file_path, chunksize=2)
    mock_read_csv.assert_called_once_with(file_path, sep=",", usecols=["col1","col2"], chunksize=2)
    assert result == mock_df

# -----------------------------
# Test lectura de archivo  CSV sin chunksize
# -----------------------------
@patch("pandas.read_csv")
def test_run_without_chunksize(mock_read_csv, csv_config):
    config, file_path = csv_config
    config["CSV"]["usar_chunk"] = False
    reader = CSVReader(config)
    mock_df = pd.DataFrame({"col1":[1,2], "col2":[3,4]})
    mock_read_csv.return_value = mock_df

    result = reader.run(file_path=file_path)
    mock_read_csv.assert_called_once_with(file_path, sep=",", usecols=["col1","col2"])
    assert result.equals(mock_df)
