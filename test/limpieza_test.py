import pandas as pd
import pytest
from src.modulos.limpieza import limpieza_df 


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'col1': [1, 2, None, 4, 4],
        'col2': ['a', 'b', 'c', 'd', 'd'],
        'fecha': ['01/01/2023', '02/01/2023', '03/01/2023', None, '05/01/2023']
    })

# ----------------------------
# Test de eliminar_nulos
# ----------------------------
def test_eliminar_nulos(sample_df):
    df_clean = limpieza_df(sample_df.copy()).eliminar_nulos(['col1']).resultado()
    assert df_clean['col1'].isnull().sum() == 0
    assert len(df_clean) == 4  

# ----------------------------
# Test de eliminar_duplicados
# ----------------------------
def test_eliminar_duplicados(sample_df):
    df_clean = limpieza_df(sample_df.copy()).eliminar_duplicados(['col1', 'col2']).resultado()
    assert len(df_clean) == 4  

# ----------------------------
# Test de convertir_datos
# ----------------------------
def test_convertir_tipos(sample_df):
    df_clean = limpieza_df(sample_df.copy()).convertir_tipos({'col1': 'float'}).resultado()
    assert df_clean['col1'].dtype == 'float64'

# ----------------------------
# Test de cambiar_tipo_fecha
# ----------------------------
def test_cambiar_tipo_fecha(sample_df):
    df_clean = limpieza_df(sample_df.copy()).cambiar_tipo_fecha(['fecha']).resultado()
    assert pd.api.types.is_datetime64_any_dtype(df_clean['fecha'])
    assert df_clean['fecha'].isnull().sum() == 1  

# ----------------------------
# Test de renombrar_columnas
# ----------------------------
def test_renombrar_columnas(sample_df):
    df_clean = limpieza_df(sample_df.copy()).renombrar_columnas({'col1': 'nueva_col1'}).resultado()
    assert 'nueva_col1' in df_clean.columns
    assert 'col1' not in df_clean.columns

# ----------------------------
# Test de resultado
# ----------------------------
def test_resultado(sample_df):
    obj = limpieza_df(sample_df.copy())
    result = obj.resultado()
    assert isinstance(result, pd.DataFrame)
    assert result.equals(sample_df)
