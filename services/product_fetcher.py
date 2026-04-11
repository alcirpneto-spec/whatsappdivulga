import csv
import re
from datetime import datetime
from pathlib import Path

from config import LINKS_PATH, SALES_DATA_PATH, NEW_LINKS_PATH


class ProductFetcher:
    def _parse_link_line(self, raw: str):
        # Preferred format: "url - nome". Fallback supports first whitespace after URL.
        if " - " in raw:
            parts = [part.strip() for part in raw.split(" - ", 1)]
            if len(parts) == 2 and parts[0].startswith("http"):
                return parts[0], parts[1]

        match = re.match(r"^(https?://\S+)\s+(.+)$", raw)
        if match:
            return match.group(1).strip(), match.group(2).strip()

        return None, None

    def load_links(self):
        links = {}
        if not LINKS_PATH.exists():
            return links

        with LINKS_PATH.open('r', encoding='utf-8') as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith('#'):
                    continue

                url, name = self._parse_link_line(raw)
                if not url or not name:
                    continue

                links[name.lower()] = url
                links[url] = url
        return links

    def load_new_links(self):
        new_links = []
        if not NEW_LINKS_PATH.exists():
            return new_links

        with NEW_LINKS_PATH.open('r', encoding='utf-8') as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith('#'):
                    continue

                url, name = self._parse_link_line(raw)
                if url and name:
                    new_links.append({'url': url, 'name': name})
        return new_links

    def load_sales_history(self):
        records = []
        if not SALES_DATA_PATH.exists():
            return records

        with SALES_DATA_PATH.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row_date = datetime.strptime(row['date'], '%Y-%m-%d').date()
                    units = int(row.get('units_sold', 0))
                    revenue = float(row.get('revenue', 0.0))
                    product_name = row.get('product_name', '').strip()

                    if product_name and units >= 0:
                        records.append({
                            'date': row_date,
                            'product_name': product_name,
                            'units_sold': units,
                            'revenue': revenue,
                        })
                except Exception:
                    continue
        return records

    def normalize_name(self, name):
        value = re.sub(r'[^0-9a-zA-ZÀ-ÿ ]+', ' ', name.lower()).strip()
        return re.sub(r'\s+', ' ', value)
