"""
ETL Scheduler using schedule library
Runs ETL job daily at configured time
"""

import time
import signal
import sys
from datetime import datetime
from typing import Optional

import schedule

from etl import run_etl, log


class ETLScheduler:
    """Simple ETL scheduler"""

    def __init__(self, cron_schedule: str = "02:00"):
        """
        Initialize scheduler

        Args:
            cron_schedule: Time to run daily (HH:MM format) or cron expression
        """
        self.schedule = cron_schedule
        self.running = False
        self.last_run: Optional[datetime] = None
        self.last_result = None

    def job(self):
        """ETL job to run"""
        if self.running:
            log('Previous ETL still running, skipping...')
            return

        self.running = True
        log(f'\n[{datetime.now().isoformat()}] Running scheduled ETL...')

        try:
            self.last_result = run_etl(force=False)
        except Exception as e:
            log(f'Scheduled ETL failed: {e}', 'ERROR')
            self.last_result = {'success': False, 'error': str(e)}
        finally:
            self.running = False
            self.last_run = datetime.now()

    def start(self):
        """Start the scheduler"""
        log(f'Starting ETL scheduler: daily at {self.schedule}')

        # Parse schedule (HH:MM format)
        try:
            schedule.every().day.at(self.schedule).do(self.job)
        except Exception:
            # Try cron format
            schedule.every().day.do(self.job)

        log('Scheduler started. Press Ctrl+C to stop.')

        # Run forever
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            log('\nShutting down...')
            sys.exit(0)

    def run_manual(self):
        """Run ETL manually"""
        if self.running:
            raise Exception('ETL already running')
        return run_etl(force=False)


def start_scheduler():
    """Start the ETL scheduler"""
    import os
    schedule_time = os.getenv('CRON_SCHEDULE', '02:00')
    scheduler = ETLScheduler(schedule_time)
    scheduler.start()


# Handle shutdown
signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))


if __name__ == '__main__':
    start_scheduler()
