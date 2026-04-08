import requests
import json
import logging
from datetime import datetime
from config import SHOPEE_APP_ID, SHOPEE_APP_SECRET, SHOPEE_AFFILIATE_ID


class ShopeeAffiliateAPI:
    def __init__(self):
        self.app_id = SHOPEE_APP_ID
        self.app_secret = SHOPEE_APP_SECRET
        self.affiliate_id = SHOPEE_AFFILIATE_ID
        self.base_url = "https://open-api.affiliate.shopee.com.br/graphql"
        self.session = requests.Session()

    def get_access_token(self):
        """Obtém token de acesso da API Shopee."""
        # Implementação simplificada - na prática, seria necessário OAuth flow
        # Este é apenas um exemplo conceitual
        return "fake_token_for_demo"

    def search_products(self, keyword, limit=10):
        """Busca produtos por palavra-chave."""
        query = """
        query {
            products(search: "%s", limit: %d) {
                items {
                    itemid
                    shopid
                    name
                    price
                    image
                    item_status
                    historical_sold
                    liked_count
                    cmt_count
                    shop_name
                }
            }
        }
        """ % (keyword, limit)

        try:
            response = self.session.post(
                self.base_url,
                json={"query": query},
                headers={"Authorization": f"Bearer {self.get_access_token()}"}
            )
            response.raise_for_status()
            data = response.json()

            products = []
            for item in data.get("data", {}).get("products", {}).get("items", []):
                products.append({
                    "id": item["itemid"],
                    "name": item["name"],
                    "price": item["price"] / 100000,  # Shopee usa centavos
                    "image": item["image"],
                    "sold": item["historical_sold"],
                    "rating": item.get("item_rating", {}).get("rating_star", 0),
                    "shop_name": item["shop_name"],
                    "url": self.generate_affiliate_link(item["itemid"], item["shopid"])
                })

            return products

        except Exception as e:
            logging.error(f"Erro na busca de produtos: {e}")
            return []

    def get_product_details(self, item_id, shop_id):
        """Obtém detalhes completos de um produto."""
        query = """
        query {
            item(itemid: %d, shopid: %d) {
                itemid
                name
                description
                price
                images
                attributes {
                    name
                    value
                }
                models {
                    name
                    price
                }
            }
        }
        """ % (item_id, shop_id)

        try:
            response = self.session.post(
                self.base_url,
                json={"query": query},
                headers={"Authorization": f"Bearer {self.get_access_token()}"}
            )
            response.raise_for_status()
            data = response.json()

            item = data.get("data", {}).get("item", {})
            return {
                "id": item["itemid"],
                "name": item["name"],
                "description": item.get("description", ""),
                "price": item["price"] / 100000,
                "images": item.get("images", []),
                "attributes": item.get("attributes", []),
                "variations": item.get("models", []),
                "url": self.generate_affiliate_link(item["itemid"], item.get("shopid", 0))
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