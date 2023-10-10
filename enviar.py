import smtplib
import time
import db
import pathlib
from email import encoders
from datetime import datetime
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime


def enviar_reporte(reporte: tuple) -> None:
    with smtplib.SMTP(host="localhost", port=25) as server:
        id = reporte[0]
        nombre = reporte[1]
        ubicacion = reporte[2]
        receiver = reporte[3]
        receiver_cc = reporte[4]
        receiver_cco = reporte[5]
        subject = reporte[6]

        zip_file = pathlib.Path(f"./reportes/{ubicacion}/{nombre}.zip")

        message = MIMEMultipart()
        message["From"] = "<no-responder@devdpma.gdeba.gba.gob.ar>"
        message["To"] = receiver
        message["Subject"] = subject
        message["Cc"] = receiver_cc

        to = receiver.split(",")

        if receiver_cc:
            to += receiver_cc.split(",")

        if receiver_cco:
            to += receiver_cco.split(",")

        body = "Reporte generado automáticamente, por favor no responder.\nContraseña: reportesdpma"
        message.attach(MIMEText(body, "plain"))

        with open(zip_file, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)

        nombre_attachment = (
            nombre + "-" + datetime.now().strftime("%d_%m_%Y_%H_%M") + ".zip"
        )

        part.add_header(
            "Content-Disposition",
            f"attachment; filename={nombre_attachment}",
        )

        message.attach(part)
        text = message.as_string()

        server.sendmail("<no-responder@devdpma.gdeba.gba.gob.ar>", to, text)
        time.sleep(1)
        db.actualizar(id, "enviado")
