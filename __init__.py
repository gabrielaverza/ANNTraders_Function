# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(__init__) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import os
import logging
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import sendgrid
from sendgrid.helpers.mail import Mail
import json

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SENDGRID_SENDER_EMAIL = os.getenv('SENDGRID_SENDER_EMAIL')
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL')

AZURE_SERVICE_BUS_CONNECTION_STRING = os.getenv('AZURE_SERVICE_BUS_CONNECTION_STRING')
QUEUE_NAME = os.getenv('QUEUE_NAME')

__init__ = func.Blueprint()

@__init__.service_bus_queue_trigger(arg_name="azservicebus", queue_name="anntraders_queue",
                                    connection="ANNTradersServiceBus_SERVICEBUS")
def ProcessProductChanges(azservicebus: func.ServiceBusMessage):
    message_body = azservicebus.get_body().decode('utf-8')
    logging.info('Mensagem received from Service Bus queue: %s', message_body)

    try:
        message_data = json.loads(message_body)
        product_id = message_data.get('product_id')

        if product_id: 
            logging.info(f"Produto ID: {product_id}") 

            send_email_notification(product_id) 

            send_message_to_service_bus(f"Product updated")
        else:
            logging.error("No product data.")
    except json.JSONDecodeError:
        logging.error("Error on Service Bus message.")

def send_email_notification(product_id, product_name):
    """
    Envia uma notificação por e-mail usando o SendGrid com detalhes da mudança do produto.
    """
    if not SENDGRID_API_KEY or not SENDGRID_SENDER_EMAIL:
        logging.error("API SendGrid key ou e-mail not configured.")
        return

    subject = f"Product updates: {product_id}"
    body = f"Product ID {product_id} was updated."

    message = Mail(
        from_email=SENDGRID_SENDER_EMAIL,
        to_emails=ADMIN_EMAIL,
        subject=subject,
        plain_text_content=body
    )

    try:
        sg = sendgrid.SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"E-mail sent: {response.status_code}")
    except Exception as e:
        logging.error(f"Error to sent e-mail: {str(e)}")

def send_message_to_service_bus(message_body):
    """
    Envia uma mensagem de confirmação para o Azure Service Bus.
    """
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=AZURE_SERVICE_BUS_CONNECTION_STRING, logging_enable=True)
    
    with servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            message = ServiceBusMessage(message_body)
            sender.send_messages(message)
    logging.info("Message sent to Azure Service Bus.")



