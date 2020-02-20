import sys
import smtplib


def send_email(msg):
    password = 'expnotification@'
    smtpObj = smtplib.SMTP('smtp.gmail.com',587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login('experiments.guo@gmail.com',password)
    smtpObj.sendmail('experiments.guo@gmail.com','zguo74@wisc.edu',"Subject: {} is done!".format(msg))
    smtpObj.quit()

if __name__ == "__main__":
    send_email(sys.argv[1])
