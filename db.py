import sqlalchemy
import configparser
import pandas
import pathlib

config = configparser.ConfigParser()
config.read("config.ini")

ORACLE_CONN_STRING = config["DATABASE"]["ORACLE_CONN_STRING"]
SQLITE_CONN_STRING = config["DATABASE"]["SQLITE_CONN_STRING"]
CHUNK_SIZE = int(config["DATABASE"]["ORACLE_CHUNK_SIZE"])

oracle_engine = sqlalchemy.create_engine(ORACLE_CONN_STRING).execution_options(
    stream_results=True
)
sqlite_engine = sqlalchemy.create_engine(SQLITE_CONN_STRING)


def ejecutar_consulta_reporte(sql_path: pathlib.Path) -> pandas.DataFrame:
    with open(sql_path) as file:
        sql_str = file.read()

    with oracle_engine.connect() as conn:
        for chunk_data in pandas.read_sql(
            sqlalchemy.text(sql_str), conn, chunksize=CHUNK_SIZE
        ):
            yield chunk_data


def generaciones_pendientes() -> list:
    with sqlite_engine.connect() as conn:
        cursor = conn.execute(
            sqlalchemy.text("select * from envios where generado = 0")
        )
        return cursor.fetchall()


def envios_pendientes() -> list:
    with sqlite_engine.connect() as conn:
        cursor = conn.execute(
            sqlalchemy.text("select * from envios where generado = 1 and enviado = 0")
        )
        return cursor.fetchall()


def inicializar() -> None:
    with sqlite_engine.connect() as conn:
        conn.execute(sqlalchemy.text("update envios set enviado = 0, generado = 0"))
        conn.commit()


def actualizar(id: int, col: str) -> None:
    with sqlite_engine.connect() as conn:
        sql = f"update envios set {col} = 1 where id = {id}"
        conn.execute(sqlalchemy.text(sql))
        conn.commit()
