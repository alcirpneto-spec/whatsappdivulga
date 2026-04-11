import json
import logging
import os
import time

import psycopg2
import schedule

from services.shopee_api import ShopeeAffiliateAPI


def get_env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def format_price_text(value) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return ""

    if numeric <= 0:
        return ""

    return f"{numeric:.2f}".replace(".", ",")


class ShopeeDiscoveryWorker:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL", "")
        self.interval_minutes = int(os.getenv("SCHEDULE_INTERVAL_MINUTES", "30"))
        self.use_shopee_api = get_env_bool("USE_SHOPEE_API", True)
        self.limit = int(os.getenv("SHOPEE_DISCOVERY_LIMIT", "10"))
        keywords_raw = os.getenv("SHOPEE_DISCOVERY_KEYWORDS", "carregador,ferramenta,eletronico,casa,beleza")
        self.keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]

        if not self.database_url:
            raise ValueError("DATABASE_URL nao definido para o worker de descoberta Shopee.")

        self.api = ShopeeAffiliateAPI()

    def run_cycle(self):
        if not self.use_shopee_api:
            logging.info("USE_SHOPEE_API=False. Ciclo de descoberta Shopee ignorado.")
            return

        if not self.keywords:
            logging.warning("Nenhuma keyword configurada para descoberta Shopee.")
            return

        inserted = 0
        skipped = 0

        conn = psycopg2.connect(self.database_url)
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                per_keyword_limit = max(1, self.limit // len(self.keywords))

                for keyword in self.keywords:
                    products = self.api.search_products(keyword=keyword, limit=per_keyword_limit)
                    logging.info("Shopee keyword '%s': %s produtos retornados", keyword, len(products))

                    for product in products:
                        url = (product.get("url") or "").strip()
                        if not url:
                            skipped += 1
                            continue

                        product_name = (product.get("name") or "Produto Shopee").strip()
                        price_text = format_price_text(product.get("price"))
                        image_url = (product.get("image") or "").strip()

                        metadata = {
                            "shop_name": (product.get("shop_name") or "").strip(),
                            "sales": product.get("sold", 0),
                            "commission_rate": str(product.get("commission_rate") or ""),
                        }

                        cur.execute(
                            """
                            INSERT INTO affiliate_links (product_name, affiliate_url, source, price_text, image_url, metadata_json)
                            SELECT %s, %s, 'shopee', NULLIF(%s, ''), NULLIF(%s, ''), %s::jsonb
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM affiliate_links
                                WHERE affiliate_url = %s
                                  AND created_at >= NOW() - INTERVAL '24 hours'
                            )
                            """,
                            (
                                product_name,
                                url,
                                price_text,
                                image_url,
                                json.dumps(metadata, ensure_ascii=False),
                                url,
                            ),
                        )

                        if cur.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1

            conn.commit()
            logging.info("Ciclo Shopee concluido. Inseridos=%s, Ignorados=%s", inserted, skipped)
        except Exception:
            conn.rollback()
            logging.exception("Erro no ciclo de descoberta Shopee")
        finally:
            conn.close()

    def run(self):
        logging.info("Iniciando worker Shopee. Intervalo=%s min, limit=%s", self.interval_minutes, self.limit)

        # Executa uma vez na partida para evitar esperar o primeiro intervalo.
        self.run_cycle()

        if self.interval_minutes <= 0:
            logging.warning("SCHEDULE_INTERVAL_MINUTES <= 0. Ajustando para 30 minutos.")
            self.interval_minutes = 30

        schedule.every(self.interval_minutes).minutes.do(self.run_cycle)

        while True:
            schedule.run_pending()
            time.sleep(30)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    worker = ShopeeDiscoveryWorker()
    worker.run()


if __name__ == "__main__":
    main()
