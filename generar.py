import db
import pathlib
import os
import pyzipper


def generar_reporte(reporte: tuple) -> None:
    id = reporte[0]
    nombre = reporte[1]
    ubicacion = reporte[2]
    sql_file = pathlib.Path(f"./reportes/{ubicacion}/{nombre}.sql")
    csv_file = pathlib.Path(f"./reportes/{ubicacion}/{nombre}.csv")
    zip_file = pathlib.Path(f"./reportes/{ubicacion}/{nombre}.zip")

    if os.path.exists(zip_file):
        os.remove(zip_file)

    resultado = db.ejecutar_consulta_reporte(sql_file)

    next(resultado).to_csv(
        csv_file, index=False, sep=";", mode="a", header=True, encoding="Windows-1252"
    )

    for restante in resultado:
        restante.to_csv(
            csv_file,
            index=False,
            sep=";",
            mode="a",
            header=False,
            encoding="Windows-1252",
        )

    with pyzipper.AESZipFile(
        file=zip_file,
        mode="w",
        compression=pyzipper.ZIP_DEFLATED,
        compresslevel=9,
        encryption=pyzipper.WZ_AES,
    ) as zf:
        zf.setpassword(b"reportesdpma")
        zf.write(filename=csv_file, arcname=csv_file.name)

    os.remove(csv_file)
    db.actualizar(id, "generado")
