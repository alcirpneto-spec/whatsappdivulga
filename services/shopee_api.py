import requests
import json
import logging
import hashlib
import time
from config import SHOPEE_APP_ID, SHOPEE_APP_SECRET, SHOPEE_AFFILIATE_ID


class ShopeeAffiliateAPI:
    def __init__(self):
        self.app_id = SHOPEE_APP_ID
        self.app_secret = SHOPEE_APP_SECRET
        self.affiliate_id = SHOPEE_AFFILIATE_ID
        self.base_url = "https://open-api.affiliate.shopee.com.br/graphql"
        self.session = requests.Session()

    def _build_authorization(self, payload_text):
        """Monta header Authorization no formato Shopee Open API.

        Assinatura: SHA256(AppId + Timestamp + Payload + Secret)
        """
        timestamp = str(int(time.time()))
        sign_factor = f"{self.app_id}{timestamp}{payload_text}{self.app_secret}"
        signature = hashlib.sha256(sign_factor.encode("utf-8")).hexdigest()
        authorization = (
            f"SHA256 Credential={self.app_id},"
            f"Timestamp={timestamp},"
            f"Signature={signature}"
        )
        return authorization

    def _execute_graphql(self, query, variables=None):
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        # O payload assinado precisa ser exatamente o payload enviado.
        payload_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/graphql-response+json, application/json",
            "Authorization": self._build_authorization(payload_text),
        }

        response = self.session.post(
            self.base_url,
            data=payload_text.encode("utf-8"),
            headers=headers,
            timeout=25,
        )
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            logging.error("Resposta invalida da Shopee Open API: %s", response.text[:500])
            return {"errors": [{"message": "Resposta JSON invalida da Shopee API"}]}

    def search_products(self, keyword, limit=10):
        """Busca produtos por palavra-chave."""
        query = """
        query ($keyword: String, $limit: Int, $page: Int) {
            productOfferV2(keyword: $keyword, limit: $limit, page: $page) {
                nodes {
                    itemId
                    productName
                    price
                    imageUrl
                    sales
                    shopName
                    productLink
                    offerLink
                    commissionRate
                }
            }
        }
        """

        try:
            data = self._execute_graphql(query, {"keyword": keyword, "limit": int(limit), "page": 1})

            if data.get("errors"):
                logging.error("Erro GraphQL na busca de produtos Shopee: %s", data["errors"])
                return []

            products = []
            for item in data.get("data", {}).get("productOfferV2", {}).get("nodes", []):
                price_number = self._parse_decimal(item.get("price"))
                products.append({
                    "id": item.get("itemId"),
                    "shopid": 0,
                    "name": item.get("productName", ""),
                    "price": price_number,
                    "image": item.get("imageUrl", ""),
                    "sold": item.get("sales", 0),
                    "rating": 0,
                    "shop_name": item.get("shopName", ""),
                    "url": item.get("offerLink") or item.get("productLink") or "",
                    "commission_rate": item.get("commissionRate", ""),
                })

            return products

        except Exception as e:
            logging.error(f"Erro na busca de produtos: {e}")
            return []

    def get_product_details(self, item_id, shop_id):
        """Obtém detalhes completos de um produto."""
        query = """
        query ($itemId: Int64) {
            productOfferV2(itemId: $itemId, limit: 1, page: 1) {
                nodes {
                itemId
                productName
                price
                imageUrl
                shopName
                productLink
                offerLink
                sales
                commissionRate
                }
            }
        }
        """

        try:
            data = self._execute_graphql(query, {"itemId": int(item_id)})

            if data.get("errors"):
                logging.error("Erro GraphQL ao obter detalhes Shopee: %s", data["errors"])
                return None

            nodes = data.get("data", {}).get("productOfferV2", {}).get("nodes", [])
            if not nodes:
                return None

            item = nodes[0]
            return {
                "id": item.get("itemId"),
                "name": item.get("productName", ""),
                "description": "",
                "price": self._parse_decimal(item.get("price")),
                "images": [item.get("imageUrl", "")] if item.get("imageUrl") else [],
                "attributes": [],
                "variations": [],
                "url": item.get("offerLink") or item.get("productLink") or "",
            }

        except Exception as e:
            logging.error(f"Erro ao obter detalhes do produto: {e}")
            return None

    def generate_affiliate_link(self, item_id, shop_id):
        """Gera link de afiliado para um produto."""
        # URL base da Shopee
        base_url = f"https://shopee.com.br/product/{shop_id}/{item_id}"

        # Adiciona parâmetros de afiliado
        affiliate_params = f"?affiliate_id={self.affiliate_id}&sub_id=whatsapp_bot"

        return base_url + affiliate_params

    def _parse_decimal(self, value):
        text = str(value or "").strip()
        if not text:
            return 0.0

        normalized = text.replace("R$", "").replace(" ", "")
        if "," in normalized and "." in normalized:
            normalized = normalized.replace(".", "").replace(",", ".")
        elif "," in normalized:
            normalized = normalized.replace(",", ".")

        try:
            return float(normalized)
        except ValueError:
            return 0.0

    def get_trending_products(self, category_id=None, limit=10):
        """Obtém produtos em tendência."""
        # Implementação para buscar produtos populares
        # Na prática, usaria endpoints específicos da API
        return self.search_products("", limit)  # Busca geral por enquanto

    def get_promotion_products(self, limit=10):
        """Obtém produtos em promoção."""
        # Busca produtos com desconto
        query = """
        query {
            products(limit: %d, sort: "sales") {
                items {
                    itemid
                    shopid
                    name
                    price
                    price_before_discount
                    discount
                    image
                }
            }
        }
        """ % limit

        # Implementação similar às outras
        return []  # Placeholder