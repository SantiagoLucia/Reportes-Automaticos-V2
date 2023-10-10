import db
import generar
import enviar
from datetime import datetime
import time
import asyncio
import logging

logging.basicConfig(
    filename="proceso.log",
    format="%(asctime)s - %(message)s",
    datefmt="%d-%m-%y %H:%M:%S",
    level=logging.INFO,
)


async def procesar_reporte(rep: tuple) -> None:
    generar.generar_reporte(rep)
    # enviar.enviar_reporte(rep)
    return rep


async def main():
    logging.info("********** INICIO DE PROCESO. **********")
    db.inicializar()
    reportes_pendientes = db.generaciones_pendientes()

    while reportes_pendientes:
        tasks = []
        for reporte in reportes_pendientes:
            hora_ejecucion = reporte[7]
            if (
                datetime.now().time()
                > datetime.strptime(hora_ejecucion, "%H:%M:%S").time()
            ):
                tasks.append(asyncio.create_task(procesar_reporte(rep=reporte)))
        results = await asyncio.gather(*tasks)
        for result in results:
            logging.info(
                f"{result[1]} -> {result[3]}, CC: {result[4]}, CCO: {result[5]}"
            )
        reportes_pendientes = db.generaciones_pendientes()
        time.sleep(10)
    logging.info("**********  FIN DE PROCESO.  ***********\n")


if __name__ == "__main__":
    asyncio.run(main())
