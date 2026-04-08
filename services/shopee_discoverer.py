import logging
from config import USE_SHOPEE_API
from .shopee_api import ShopeeAffiliateAPI
from .analyzer import SalesAnalyzer


class ShopeeProductDiscoverer:
    def __init__(self):
        self.api = ShopeeAffiliateAPI()
        self.analyzer = SalesAnalyzer()

    def discover_and_analyze(self, keywords=None, limit=5):
        """Descobre produtos automaticamente e analisa para divulgação."""
        if not USE_SHOPEE_API:
            logging.info("API Shopee desabilitada. Usando modo manual.")
            return []

        if keywords is None:
            keywords = ["carregador", "ferramenta", "eletrônico", "casa", "beleza"]

        all_products = []

        for keyword in keywords:
            logging.info(f"Buscando produtos para: {keyword}")
            products = self.api.search_products(keyword, limit=limit//len(keywords) or 1)

            for product in products:
                # Enriquecer com detalhes completos
                details = self.api.get_product_details(product["id"], product.get("shopid", 0))
                if details:
                    product.update(details)

                # Adicionar métricas de "popularidade"
                product["popularity_score"] = self._calculate_popularity_score(product)
                all_products.append(product)

        # Ordenar por popularidade e limitar
        best_products = sorted(all_products, key=lambda x: x["popularity_score"], reverse=True)[:limit]

        # Converter para formato compatível com analyzer
        formatted_products = []
        for i, product in enumerate(best_products, start=1):
            formatted_products.append({
                'rank': i,
                'product_name': product['name'],
                'url': product['url'],
                'description': product.get('description', 'Produto incrível em oferta!'),
                'price': product.get('price', 0),
                'sold': product.get('sold', 0),
                'rating': product.get('rating', 0),
                'why': self._build_reason(product)
            })

        return formatted_products

    def _calculate_popularity_score(self, product):
        """Calcula score de popularidade baseado em vendas, rating, etc."""
        sold_score = min(product.get('sold', 0) / 100, 10)  # Máximo 10 pontos
        rating_score = product.get('rating', 0) * 2  # Rating de 0-5 vira 0-10
        return sold_score + rating_score

    def _build_reason(self, product):
        sold = product.get('sold', 0)
        rating = product.get('rating', 0)
        price = product.get('price', 0)

        reasons = []
        if sold > 100:
            reasons.append(f"Mais de {sold} vendas")
        if rating >= 4.5:
            reasons.append(f"Avaliação {rating:.1f}⭐")
        if price > 0:
            reasons.append(f"Preço: R${price:.2f}")

        return " | ".join(reasons) if reasons else "Produto em destaque na Shopee"

    def get_trending_products(self, limit=5):
        """Obtém produtos em tendência automaticamente."""
        return self.api.get_trending_products(limit=limit)

    def get_promotional_products(self, limit=5):
        """Obtém produtos em promoção."""
        return self.api.get_promotion_products(limit=limit)