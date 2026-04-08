from collections import defaultdict
from datetime import date, timedelta

from config import TOP_K, MAX_MESSAGE_ITEMS
from .product_scraper import ProductScraper


class SalesAnalyzer:
    def __init__(self):
        self.scraper = ProductScraper()

    def find_best_sellers(self, sales_records, links, window_days=7):
        cutoff = date.today() - timedelta(days=window_days)
        score = defaultdict(lambda: {'units': 0, 'revenue': 0.0, 'dates': set()})

        for item in sales_records:
            if item['date'] < cutoff:
                continue

            key = item['product_name'].strip()
            score[key]['units'] += item['units_sold']
            score[key]['revenue'] += item['revenue']
            score[key]['dates'].add(item['date'])

        best = sorted(
            [
                {
                    'product_name': name,
                    'units_sold': data['units'],
                    'revenue': data['revenue'],
                    'days_sold': len(data['dates']),
                }
                for name, data in score.items()
                if data['units'] > 0
            ],
            key=lambda item: (item['units_sold'], item['revenue']),
            reverse=True,
        )[:TOP_K]

        for rank, item in enumerate(best, start=1):
            item['rank'] = rank
            item['score'] = item['units_sold'] * 1.0 + item['revenue'] / 100.0

        for item in best:
            item['url'] = self._find_link(item['product_name'], links)
            item['description'] = self.scraper.get_product_description(item['url'])
            item['why'] = self._build_reason(item)

        return best

    def get_new_links_messages(self, new_links):
        messages = []
        for i, item in enumerate(new_links[:MAX_MESSAGE_ITEMS], start=1):
            info = self.scraper.get_product_info(item['url'])
            product_name = item['name'] or info.get('name') or f"Produto {i}"
            message = self.build_message({
                'rank': i,
                'product_name': product_name,
                'url': info.get('url', item['url']),
                'description': info.get('description', "Produto em destaque para divulgação!"),
                'why': "Novo produto em destaque para divulgação!"
            })
            messages.append(message)
        return messages

    def _find_link(self, product_name, links):
        normalized = product_name.lower()
        exact = links.get(normalized)
        if exact:
            return exact

        for key, url in links.items():
            if key in normalized and url.startswith('http'):
                return url

        return 'Link não encontrado'

    def _build_reason(self, item):
        return (
            f"Vendido {item['units_sold']} vezes nos últimos dias, com faturamento aproximado de R${item['revenue']:.2f}."
        )

    def build_message(self, item):
        lines = [
            f"🔥 Produto #{item['rank']}: {item['product_name']}",
            f"📝 Descrição: {item.get('description', 'Produto incrível!')}",
            f"📈 Motivo: {item['why']}",
            f"🔗 Link: {item['url']}",
        ]
        return '\n'.join(lines)
