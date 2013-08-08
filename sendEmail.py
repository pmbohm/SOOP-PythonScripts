# -*- coding: utf-8 -*-

import smtplib
#from email.mime.multipart import MIMEMultipart
#from email.mime.text import MIMEText
from email.MIMEText import MIMEText
from BeautifulSoup import BeautifulSoup
import re

class sendEmail:
        """
        
                to suit python 2.4.3
        
        """

        def __init__(self):
                pass
        
        def sendEmail(self,to,subject,body):

                sender = "python_cron@emii.org.au"
                
                # Create the body of the message (a plain-text and an HTML version).
                body_astext = self.strip_tags(body)
                
                #html = """\
                #<html>
                #  <head></head>
                #  <body>
                #   <p>""" + body + """\
                #    </p>
                #  </body>
                #</html>
                #"""
                
                
                # Create message container - the correct MIME type is multipart/alternative.
                #msg = MIMEMultipart('alternative')
                msg = MIMEText(body_astext)
                msg['Subject'] = subject
                msg['From'] = sender
                msg['To'] = to



                # Record the MIME types of both parts - text/plain and text/html.
                #part1 = MIMEText(body_astext, 'plain')
                #part2 = MIMEText(body, 'html')

                # Attach parts into message container.
                # According to RFC 2046, the last part of a multipart message, in this case
                # the HTML message, is best and preferred.
                #msg.attach(part1)
                #msg.attach(part2)

                # Send the message via local SMTP server.
                s = smtplib.SMTP('localhost')
                # sendmail function takes 3 arguments: sender's address, recipient's address
                # and message to send - here it is sent as one string.
                s.sendmail(sender, to, msg.as_string())
                s.quit()
                
        def strip_tags(self,content):
                
                VALID_TAGS = 'div', 'p'
                #print content
                soup = BeautifulSoup(content)

                for tag in soup.findAll(True):
                    if tag.name not in VALID_TAGS:
                        tag.replaceWith(tag.renderContents())

                return soup.renderContents()

                
if __name__ == "__main__":
        email = sendEmail()
        email.sendEmail("pmbohm@gmail.com","test","this is a test of my plain  email class to suit python 2.4.3")