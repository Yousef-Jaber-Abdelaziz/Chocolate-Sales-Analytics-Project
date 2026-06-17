from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime

# --- CONFIGURATION ---
# 1. This MUST be the email you verified in your SendGrid "Single Sender Authentication"
VERIFIED_SENDER = 'gabery686p@gmail.com' 

# 2. This is where you want to receive the test email
RECIPIENT_EMAIL = 'gabery686p@gmail.com' 

default_args = {
    'owner': 'yousef',
    'start_date': datetime(2026, 4, 28),
    'retries': 0
}

with DAG(
    'test_email_connection',
    default_args=default_args,
    description='Manual test for SendGrid SMTP connection',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['debug', 'smtp']
) as dag:

    send_test_email = EmailOperator(
        task_id='execute_sendgrid_test',
        conn_id='smtp_default',
        from_email=VERIFIED_SENDER,
        to=RECIPIENT_EMAIL,
        subject='Airflow 3.0 SMTP Test: SendGrid Success',
        html_content=f"""
        <h3>Connection Verified!</h3>
        <p>Your Airflow instance successfully talked to SendGrid.</p>
        <ul>
            <li><b>Sender:</b> {VERIFIED_SENDER}</li>
            <li><b>Recipient:</b> {RECIPIENT_EMAIL}</li>
            <li><b>Timestamp:</b> {{{{ ts }}}}</li>
            <li><b>Run ID:</b> {{{{ run_id }}}}</li>
        </ul>
        <p><i>You can now safely use these settings in your main Chocolate Ingestion pipeline.</i></p>
        """
    )
    send_test_email