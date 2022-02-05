import time
import smtplib
import traceback

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def mail_error_sender(error, API):

    name = API["name"]

    try:
        subject = name + " dostal errorem, przypiety log"
        traceback_log = traceback.format_exc()
        message = str(traceback_log)
        print(traceback_log)

        with open("Logi.txt", "r") as U:
            lines = U.readlines()
        lines = [Line.replace("\x00", "") for Line in lines]
        with open("Logi.txt", "w") as U:
            U.writelines(lines)
        print("Cleaned logs from nulls before sending")

        msg = MIMEMultipart("alternative")
        part_1 = MIMEBase("application", "octet-stream")
        part_1.set_payload(open("Logi.txt", "rb").read())
        encoders.encode_base64(part_1)
        part_1.add_header('Content-Disposition', 'attachment; filename = "Logi.txt"')
        part_2 = MIMEText(message, "plain")
        msg.attach(part_1)
        msg.attach(part_2)
        msg["subject"] = str(subject)
        msg["From"] = "BotOverseer69@gmail.com"
        msg["To"] = "mrkalindro@gmail.com"

        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login("BotOverseer69@gmail.com", "kxruyramgmaldlpt")
        server.send_message(msg)
        server.quit()
        print(f"Error: {error}, E-mail sent")

    except Exception as wtf:
        print("Problem nawet z wysłaniem maila, Error: \n", wtf)
        time.sleep(30)
