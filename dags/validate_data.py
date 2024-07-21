from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import mysql.connector
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def get_mysql_count(table_name):
    conn = mysql.connector.connect(
        host="mysql",
        user="user",
        password="12345678",
        database="ecommerce"
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    result = cursor.fetchone()
    conn.close()
    return result[0]

def get_clickhouse_count(table_name):
    client = Client(host='clickhouse', port=9000)
    result = client.execute(f"SELECT COUNT(*) FROM ecommerce.{table_name}")
    return result[0][0]

def validate_counts():
    tables = ['products', 'customers', 'order_items', 'orders']
    tolerance = 0.05  # 5% tolerance

    discrepancies = []

    for table in tables:
        mysql_count = get_mysql_count(table)
        clickhouse_count = get_clickhouse_count(table)

        logger.info(f"{table}: MySQL count = {mysql_count}, ClickHouse count = {clickhouse_count}")

        if abs(mysql_count - clickhouse_count) / mysql_count > tolerance:
            discrepancies.append((table, mysql_count, clickhouse_count))

    if discrepancies:
        send_email(discrepancies)

def send_email(discrepancies):
    smtp_server = 'smtp.example.com'
    smtp_port = 587
    smtp_user = 'your-email@example.com'
    smtp_password = 'your-email-password'

    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = 'recipient@example.com'
    msg['Subject'] = 'Data Validation Discrepancy Detected'

    body = 'The following tables have discrepancies in row counts between MySQL and ClickHouse:\n\n'
    for table, mysql_count, clickhouse_count in discrepancies:
        body += f"Table: {table}, MySQL count: {mysql_count}, ClickHouse count: {clickhouse_count}\n"

    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.send_message(msg)
    server.quit()

with DAG('validate_data', default_args=default_args, schedule_interval='@daily') as dag:
    validate_data_task = PythonOperator(
        task_id='validate_data_task',
        python_callable=validate_counts,
    )
