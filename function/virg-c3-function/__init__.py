import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    POSTGRES_URL = "virg-c3-dbserver.postgres.database.azure.com"
    POSTGRES_USER = "udacityadmin@virg-c3-dbserver"
    POSTGRES_PW = "My-password"
    POSTGRES_DB = "techconfdb"
    ADMIN_EMAIL_ADDRESS = 'info@techconf.com'
    SENDGRID_API_KEY = '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"'

    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PW,
        host=POSTGRES_URL)
    logging.info("Connected to database")

    try:
        curs = conn.cursor()

        # TODO: Get notification message and subject from database using the notification_id
        notification = curs.execute("SELECT message, subject FROM notification WHERE id = {};".format(notification_id))
        logging.info("Get notification")

        # TODO: Get attendees email and name
        curs.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = curs.fetchall()
        logging.info('Get attendees {}'.format(len(attendees)))

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            subject = 'Notification for {} {}'.format(attendee[0], attendee[1])
            message = Mail(
                from_email=ADMIN_EMAIL_ADDRESS,
                to_emails=attendee[2],
                subject=subject,
                plain_text_content=notification)
            sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
            response = sg.send(message)
            logging.info('Sending mail: {}'.format(subject))
        
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        completed = datetime.utcnow()
        status = 'Notified {} attendees'.format(len(attendees))
        curs.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(status, completed, notification_id))        
        conn.commit()
        logging.info('Database updated')
        

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        curs.close()
        conn.close()