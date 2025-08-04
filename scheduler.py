import schedule
import time
from region_demand import main

#Schedule the task to run every day at 01:00 AM
schedule.every().day.at("01:00").do(main)

print("Scheduler is runnning (Press ctrl+C to stop)")
while True:
    schedule.run_pending()
    time.sleep(60)
