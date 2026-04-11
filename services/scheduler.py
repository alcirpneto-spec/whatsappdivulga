import logging
import time

import schedule

from config import CHECK_TIME, SCHEDULE_INTERVAL_MINUTES, USE_SHOPEE_API
from .agent import DailyMarketingAgent


def run_daily_scheduler():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    agent = DailyMarketingAgent()

    if USE_SHOPEE_API and SCHEDULE_INTERVAL_MINUTES > 0:
        schedule.every(SCHEDULE_INTERVAL_MINUTES).minutes.do(agent.run_once)
        logging.info(
            f'API Shopee habilitada: agendamento a cada {SCHEDULE_INTERVAL_MINUTES} minutos. Iniciando loop...'
        )
    else:
        schedule.every().day.at(CHECK_TIME).do(agent.run_once)
        logging.info(f'Agendamento diário configurado para {CHECK_TIME}. Iniciando loop...')

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == '__main__':
    run_daily_scheduler()
