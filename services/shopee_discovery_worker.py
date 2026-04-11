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


def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip().replace(",", "."))
    except ValueError:
        return default


def get_env_int_list(name: str):
    value = os.getenv(name, "")
    items = []
    for raw in value.split(","):
        token = raw.strip()
        if not token:
            continue
        try:
            items.append(int(token))
        except ValueError:
            continue
    return items


def format_price_text(value) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return ""

    if numeric <= 0:
        return ""

    return f"{numeric:.2f}".replace(".", ",")


def normalize_url(url: str) -> str:
    clean = (url or "").strip()
    if not clean:
        return ""
    base = clean.split("?", 1)[0].rstrip("/")
    return base.lower()


def parse_commission_rate(value) -> float:
    text = str(value or "").strip().replace("%", "")
    if not text:
        return 0.0

    text = text.replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        value_num = float(text)
    except ValueError:
        return 0.0

    # Em alguns retornos pode vir fracao (0.12) e em outros percentual (12)
    if 0 < value_num <= 1:
        value_num *= 100

    return max(0.0, value_num)


class ShopeeDiscoveryWorker:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL", "")
        self.interval_minutes = get_env_int("SCHEDULE_INTERVAL_MINUTES", 30)
        self.use_shopee_api = get_env_bool("USE_SHOPEE_API", True)
        self.limit = get_env_int("SHOPEE_DISCOVERY_LIMIT", 10)
        self.list_type = get_env_int("SHOPEE_LIST_TYPE", 0)
        self.is_top_performing_mode = self.list_type == 2
        keywords_raw = os.getenv("SHOPEE_DISCOVERY_KEYWORDS", "carregador,ferramenta,eletronico,casa,beleza")
        self.keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
        self.min_sales = get_env_int("SHOPEE_FILTER_MIN_SALES", 5)
        self.min_price = get_env_float("SHOPEE_FILTER_MIN_PRICE", 20.0)
        self.max_price = get_env_float("SHOPEE_FILTER_MAX_PRICE", 500.0)
        self.min_commission_rate = get_env_float("SHOPEE_FILTER_MIN_COMMISSION_RATE", 0.0)
        self.dedup_hours = max(0, get_env_int("SHOPEE_DEDUP_HOURS", 168))
        self.top_max_pages = max(1, get_env_int("SHOPEE_TOP_MAX_PAGES", 2))
        self.max_products_per_cycle = max(1, get_env_int("SHOPEE_MAX_PRODUCTS_PER_CYCLE", 30))
        self.api_call_delay_seconds = max(0.0, get_env_float("SHOPEE_API_CALL_DELAY_SECONDS", 0.5))
        self.allowed_category_ids = set(get_env_int_list("SHOPEE_FILTER_ALLOWED_CATEGORY_IDS"))

        if not self.database_url:
            raise ValueError("DATABASE_URL nao definido para o worker de descoberta Shopee.")

        self.api = ShopeeAffiliateAPI()

    def _should_skip_product(self, product: dict):
        sales = product.get("sold", 0)
        price = product.get("price", 0)
        category_id = product.get("category_id")
        category_ids = product.get("category_ids") or []

        if self.allowed_category_ids:
            normalized_category_ids = set()
            for value in category_ids:
                try:
                    normalized_category_ids.add(int(value))
                except (TypeError, ValueError):
                    continue

            try:
                normalized_category_ids.add(int(category_id))
            except (TypeError, ValueError):
                pass

            if not normalized_category_ids:
                return True, "missing_category"

            if not normalized_category_ids.intersection(self.allowed_category_ids):
                return True, "category_not_allowed"

        try:
            sales_num = int(sales)
        except (TypeError, ValueError):
            sales_num = 0

        if sales_num < self.min_sales:
            return True, "min_sales"

        try:
            price_num = float(price)
        except (TypeError, ValueError):
            price_num = 0.0

        if price_num < self.min_price:
            return True, "min_price"

        if self.max_price > 0 and price_num > self.max_price:
            return True, "max_price"

        commission_rate = parse_commission_rate(product.get("commission_rate"))
        if self.min_commission_rate > 0 and commission_rate < self.min_commission_rate:
            return True, "min_commission_rate"

        return False, ""

    def _build_search_batches(self, search_terms):
        if self.is_top_performing_mode:
            return [("", None, page) for page in range(1, self.top_max_pages + 1)]

        batches = []
        for keyword in search_terms:
            if self.allowed_category_ids:
                category_ids = sorted(self.allowed_category_ids)
            else:
                category_ids = [None]

            for category_id in category_ids:
                batches.append((keyword, category_id, 1))

        return batches

    def run_cycle(self):
        if not self.use_shopee_api:
            logging.info("USE_SHOPEE_API=False. Ciclo de descoberta Shopee ignorado.")
            return

        if self.is_top_performing_mode:
            logging.info(
                "Modo TOP_PERFORMING ativo (listType=2). O worker vai ignorar keyword/categoria "
                "no request e aplicar filtros localmente."
            )
            search_terms = [""]
        elif not self.keywords:
            if self.list_type > 0:
                logging.info(
                    "SHOPEE_DISCOVERY_KEYWORDS vazio. Usando descoberta global por listType=%s.",
                    self.list_type,
                )
                search_terms = [""]
            else:
                logging.warning("Nenhuma keyword configurada para descoberta Shopee.")
                return
        else:
            search_terms = self.keywords

        inserted = 0
        skipped = 0
        skipped_by_filter = {
            "missing_category": 0,
            "category_not_allowed": 0,
            "min_sales": 0,
            "min_price": 0,
            "max_price": 0,
            "min_commission_rate": 0,
            "duplicate_in_cycle_item": 0,
            "duplicate_in_cycle_url": 0,
            "missing_url": 0,
            "duplicate_in_db": 0,
            "other_filter": 0,
        }
        seen_item_ids = set()
        seen_urls = set()

        conn = psycopg2.connect(self.database_url)
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                search_batches = self._build_search_batches(search_terms)
                per_batch_limit = max(1, min(self.limit, self.max_products_per_cycle))
                processed_in_cycle = 0

                for keyword, category_id, page in search_batches:
                    if processed_in_cycle >= self.max_products_per_cycle:
                        logging.info(
                            "Limite de processamento por ciclo atingido (%s produtos).",
                            self.max_products_per_cycle,
                        )
                        break

                    remaining_in_cycle = max(1, self.max_products_per_cycle - processed_in_cycle)
                    current_limit = min(per_batch_limit, remaining_in_cycle)

                    products = self.api.search_products(
                        keyword=keyword,
                        limit=current_limit,
                        category_id=category_id,
                        list_type=self.list_type if self.list_type > 0 else None,
                        page=page,
                    )

                    label = keyword if keyword else "(global)"
                    if self.is_top_performing_mode:
                        logging.info(
                            "Shopee TOP_PERFORMING page=%s: %s produtos retornados",
                            page,
                            len(products),
                        )
                    elif category_id is None:
                        logging.info("Shopee termo '%s': %s produtos retornados", label, len(products))
                    else:
                        logging.info(
                            "Shopee termo '%s' categoria %s: %s produtos retornados",
                            label,
                            category_id,
                            len(products),
                        )

                    if self.api_call_delay_seconds > 0:
                        time.sleep(self.api_call_delay_seconds)

                    for product in products:
                        if processed_in_cycle >= self.max_products_per_cycle:
                            break

                        should_skip, reason = self._should_skip_product(product)
                        if should_skip:
                            skipped += 1
                            if reason in skipped_by_filter:
                                skipped_by_filter[reason] += 1
                            else:
                                skipped_by_filter["other_filter"] += 1
                            continue

                        item_id_raw = product.get("id")
                        try:
                            item_id_text = str(int(item_id_raw)) if item_id_raw is not None else ""
                        except (TypeError, ValueError):
                            item_id_text = ""

                        if item_id_text:
                            if item_id_text in seen_item_ids:
                                skipped += 1
                                skipped_by_filter["duplicate_in_cycle_item"] += 1
                                continue
                            seen_item_ids.add(item_id_text)

                        url = (product.get("url") or "").strip()
                        if not url:
                            skipped += 1
                            skipped_by_filter["missing_url"] += 1
                            continue

                        canonical_url = normalize_url(url)
                        if canonical_url in seen_urls:
                            skipped += 1
                            skipped_by_filter["duplicate_in_cycle_url"] += 1
                            continue
                        seen_urls.add(canonical_url)

                        product_name = (product.get("name") or "Produto Shopee").strip()
                        price_text = format_price_text(product.get("price"))
                        image_url = (product.get("image") or "").strip()

                        metadata = {
                            "item_id": item_id_text,
                            "shop_name": (product.get("shop_name") or "").strip(),
                            "sales": product.get("sold", 0),
                            "commission_rate": str(product.get("commission_rate") or ""),
                            "category_id": product.get("category_id"),
                            "category_ids": product.get("category_ids") or [],
                            "category_name": (product.get("category_name") or "").strip(),
                            "canonical_url": canonical_url,
                            "list_type": self.list_type,
                        }

                        if self.dedup_hours > 0:
                            cur.execute(
                                """
                                INSERT INTO affiliate_links (product_name, affiliate_url, source, price_text, image_url, metadata_json)
                                SELECT %s, %s, 'shopee', NULLIF(%s, ''), NULLIF(%s, ''), %s::jsonb
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM affiliate_links
                                    WHERE source = 'shopee'
                                      AND created_at >= NOW() - (%s || ' hours')::interval
                                      AND (
                                        (%s <> '' AND metadata_json->>'item_id' = %s)
                                        OR lower(rtrim(split_part(affiliate_url, '?', 1), '/')) = %s
                                      )
                                )
                                """,
                                (
                                    product_name,
                                    url,
                                    price_text,
                                    image_url,
                                    json.dumps(metadata, ensure_ascii=False),
                                    self.dedup_hours,
                                    item_id_text,
                                    item_id_text,
                                    canonical_url,
                                ),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO affiliate_links (product_name, affiliate_url, source, price_text, image_url, metadata_json)
                                SELECT %s, %s, 'shopee', NULLIF(%s, ''), NULLIF(%s, ''), %s::jsonb
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM affiliate_links
                                    WHERE source = 'shopee'
                                      AND (
                                        (%s <> '' AND metadata_json->>'item_id' = %s)
                                        OR lower(rtrim(split_part(affiliate_url, '?', 1), '/')) = %s
                                      )
                                )
                                """,
                                (
                                    product_name,
                                    url,
                                    price_text,
                                    image_url,
                                    json.dumps(metadata, ensure_ascii=False),
                                    item_id_text,
                                    item_id_text,
                                    canonical_url,
                                ),
                            )

                        if cur.rowcount > 0:
                            inserted += 1
                            processed_in_cycle += 1
                        else:
                            skipped += 1
                            skipped_by_filter["duplicate_in_db"] += 1

            conn.commit()
            logging.info(
                "Ciclo Shopee concluido. Inseridos=%s, Ignorados=%s, Filtros=%s",
                inserted,
                skipped,
                skipped_by_filter,
            )
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
