from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
from clickhouse_driver import Client

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def load_denormalized_orders():
    try:
        # Directly connect to ClickHouse using hardcoded values
        client = Client(host='clickhouse', port=9000)
        logger.info("Connected to ClickHouse")

        # Example query to load data
        client.execute('''
            INSERT INTO ecommerce.denormalized_orders (
                order_id, customer_id, order_created_at, order_item_id, 
                product_id, quantity, unit_price, order_item_created_at, day_id)
            SELECT 
                o.order_id, 
                o.customer_id, 
                o.created_at as order_created_at,
                oi.order_item_id,
                oi.product_id, 
                oi.quantity, 
                oi.unit_price, 
                oi.created_at as order_item_created_at,
                toString(today()) as day_id
            FROM 
                ecommerce.orders o
            INNER JOIN 
                ecommerce.order_items oi 
            ON 
                o.order_id = oi.order_id
            
        ''')
        logger.info("Data successfully inserted into denormalized_orders")
        count = client.execute('''
            select count(*) from ecommerce.denormalized_orders

        ''')
        logger.info(f"denormalized_orders = {count}")
    except Exception as e:
        logger.error(f"Error loading data into denormalized_orders: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('load_denormalized_orders', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    load_denormalized_orders_task = PythonOperator(
        task_id='load_denormalized_orders_task',
        python_callable=load_denormalized_orders
    )
