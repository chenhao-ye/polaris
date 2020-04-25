import sys
import smtplib


def send_email(msg):
    password = 'experiment'
    smtpObj = smtplib.SMTP('smtp.gmail.com',587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login('exp.zeng@gmail.com',password)
    smtpObj.sendmail('exp.zeng@gmail.com','xzeng56@wisc.edu',"Subject: {} is done!".format(msg))
    smtpObj.quit()

if __name__ == "__main__":
    send_email(sys.argv[1])
