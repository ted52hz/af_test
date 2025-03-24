# dags/telegram_notification_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import telegram
import requests

# Telegram configs
TELEGRAM_BOT_TOKEN = 
TELEGRAM_CHAT_ID = 

default_args = {
    'owner': 'khoaht',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'email': ['],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def send_telegram_message():
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": "🔔 Hello from Airflow!",
        "parse_mode": "Markdown"
    }
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=payload)
    except Exception as e:
        print(f"Failed to send Telegram message: {str(e)}")

# Use with statement
with DAG(
    'telegram_notification_basic',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    send_notification = PythonOperator(
        task_id='send_telegram_notification',
        python_callable=send_telegram_message,
    )
