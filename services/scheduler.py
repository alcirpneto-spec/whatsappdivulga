import logging
import time

import schedule

from config import CHECK_TIME
from .agent import DailyMarketingAgent


def run_daily_scheduler():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    agent = DailyMarketingAgent()
    schedule.every().day.at(CHECK_TIME).do(agent.run_once)

    logging.info(f'Agendamento diário configurado para {CHECK_TIME}. Iniciando loop...')
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == '__main__':
    run_daily_scheduler()
