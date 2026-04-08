import logging
from services.agent import DailyMarketingAgent

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def main():
    agent = DailyMarketingAgent()
    agent.run_once()


if __name__ == "__main__":
    main()
