"""
write.write()
Implementa la persistencia en Postgres usando la conexión configurada en Airflow env.
Debe manejar idempotencia usando UNIQUE (title, created_at, external_link).
"""
from typing import Optional

def write(input_csv_path: str, db_conn_str: str) -> bool:
    """
    Insertar filas de input_csv_path en la DB indicada por db_conn_str.
    Retornar True si todo ok, False si hubo error.
    """
    # TODO: implementar upsert/skip respectando la clave compuesta
    return False
