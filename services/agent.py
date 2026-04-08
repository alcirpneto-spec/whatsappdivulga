import logging

from config import USE_WHATSAPP_AUTOMATION, USE_SHOPEE_API
from .product_fetcher import ProductFetcher
from .analyzer import SalesAnalyzer
from .shopee_discoverer import ShopeeProductDiscoverer
from .whatsapp_sender import WhatsAppSender


class DailyMarketingAgent:
    def __init__(self):
        self.fetcher = ProductFetcher()
        self.analyzer = SalesAnalyzer()
        self.shopee_discoverer = ShopeeProductDiscoverer()
        self.sender = WhatsAppSender()

    def run_once(self):
        logging.info("Iniciando análise diária de produtos...")

        if USE_SHOPEE_API:
            # Modo automático: descobrir produtos via API Shopee
            logging.info("Usando API Shopee para descoberta automática de produtos...")
            best_products = self.shopee_discoverer.discover_and_analyze()

            if not best_products:
                logging.error("Nenhum produto descoberto via API Shopee.")
                return

        else:
            # Modo manual: usar links fornecidos ou análise de vendas
            new_links = self.fetcher.load_new_links()
            if new_links:
                logging.info(f"Encontrados {len(new_links)} novos links para divulgação.")
                messages = self.analyzer.get_new_links_messages(new_links)
            else:
                logging.info("Nenhum novo link encontrado. Analisando vendas históricas...")
                links = self.fetcher.load_links()
                sales = self.fetcher.load_sales_history()
                if not sales:
                    logging.error("Nenhum registro de vendas encontrado. Verifique data/sales_data.csv.")
                    return

                best_products = self.analyzer.find_best_sellers(sales, links)
                if not best_products:
                    logging.error("Nenhum produto com vendas suficiente para análise.")
                    return

                messages = [self.analyzer.build_message(item) for item in best_products]
        full_message = "\n\n".join(messages)

        logging.info("Mensagem gerada para envio:")
        logging.info(full_message)

        if USE_WHATSAPP_AUTOMATION:
            self.sender.send_group_message(full_message)
        else:
            logging.info("Automação WhatsApp desabilitada. Mensagem pronta acima.")
