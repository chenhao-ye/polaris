import sys
import smtplib

zhihan_send = "experiments.guo@gmail.com"
zhihan_recv = "zhihan@cs.wisc.edu"
zhihan_pass = 'expnotification@'

sender = zhihan_send
recver = zhihan_recv
password = zhihan_pass

def send_email(msg):
    smtpObj = smtplib.SMTP('smtp.gmail.com',587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(sender, password)
    smtpObj.sendmail(sender, recver, "Subject: {} is done!".format(msg))
    smtpObj.quit()

if __name__ == "__main__":
    send_email(sys.argv[1])
