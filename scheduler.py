import schedule
import time
from region_demand import main
from datetime import datetime
import pytz
import requests

# Slack Webhook URL
SLACK_WEBHOOK_URL = "Your Slack Webhook URL"

def send_slack_message(message):
    try:
        payload = {"text": message}
        requests.post(SLACK_WEBHOOK_URL, json=payload)
    except Exception as e:
        print(f"Slack sending error: {e}")

def run_main_with_notice():
    jst = pytz.timezone("Asia/Tokyo")
    now_str = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    send_slack_message(f"Started Scheduler running: {now_str}")
    main()

#Run three times a day
schedule.every().day.at("01:00").do(run_main_with_notice)
schedule.every().day.at("09:00").do(run_main_with_notice)
schedule.every().day.at("17:00").do(run_main_with_notice)

print("Scheduler is running (Press ctrl+C to stop)")
send_slack_message("Scheduler is running")

while True:
    schedule.run_pending()
    time.sleep(60)
