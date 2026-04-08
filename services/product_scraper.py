import requests
from bs4 import BeautifulSoup
import logging
import re
from urllib.parse import urlparse


class ProductScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_product_description(self, url, max_length=150):
        """Extrai uma breve descrição do produto da página."""
        try:
            final_url, soup = self._get_soup(url)
            description = self._extract_description(soup, final_url)

            if description:
                description = self._clean_description(description, max_length)
                return description

        except Exception as e:
            logging.warning(f"Erro ao extrair descrição de {url}: {e}")

        return "Produto em destaque - confira as especificações no link!"

    def get_product_info(self, url, max_length=150):
        """Resolve o link, obtém título, descrição e URL final do produto."""
        try:
            final_url, soup = self._get_soup(url)
            title = self._extract_title(soup, final_url)
            description = self._extract_description(soup, final_url)

            if description:
                description = self._clean_description(description, max_length)
            else:
                description = "Produto em destaque - confira as especificações no link!"

            return {
                'url': final_url,
                'name': title or None,
                'description': description,
            }
        except Exception as e:
            logging.warning(f"Erro ao obter informações do produto de {url}: {e}")
            return {
                'url': url,
                'name': None,
                'description': "Produto em destaque - confira as especificações no link!",
            }

    def _get_soup(self, url, timeout=10):
        final_url = self.resolve_url(url, timeout)
        response = self.session.get(final_url, timeout=timeout)
        response.raise_for_status()
        return final_url, BeautifulSoup(response.content, 'html.parser')

    def resolve_url(self, url, timeout=10):
        try:
            response = self.session.head(url, allow_redirects=True, timeout=timeout)
            if response.status_code >= 400 or not response.url:
                response = self.session.get(url, allow_redirects=True, timeout=timeout)
            return response.url
        except Exception:
            return url

    def get_product_title(self, url):
        try:
            final_url, soup = self._get_soup(url)
            title = self._extract_title(soup, final_url)
            return title
        except Exception:
            return None

    def _extract_description(self, soup, final_url):
        """Tenta extrair descrição usando vários métodos."""
        domain = urlparse(final_url).netloc.lower()

        if 'amazon.' in domain:
            amazon_desc = self._extract_amazon_description(soup)
            if amazon_desc:
                return amazon_desc

        if 'mercadolivre.' in domain or 'mercadolibre.' in domain or 'meli.la' in domain:
            ml_desc = self._extract_mercado_livre_description(soup)
            if ml_desc:
                return ml_desc

        # Método 1: Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content']

        # Método 2: Open Graph description
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc and og_desc.get('content'):
            return og_desc['content']

        # Método 3: Títulos e textos comuns em e-commerce
        selectors = [
            'h1',  # Título principal
            '.product-title',
            '.product-name',
            '.item-title',
            '.title',
            '.product-description',
            '.description',
            '.item-description',
            'p',  # Primeiro parágrafo
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                text = elements[0].get_text(strip=True)
                if len(text) > 20:  # Evita textos muito curtos
                    return text

        # Método 4: Primeiro texto significativo da página
        text_elements = soup.find_all(text=True)
        for element in text_elements:
            text = element.strip()
            if len(text) > 50 and not text.startswith(('http', 'www', '©', 'All rights')):
                return text

        return None

    def _extract_title(self, soup, final_url):
        domain = urlparse(final_url).netloc.lower()

        if 'amazon.' in domain:
            selectors = ['#productTitle', '.product-title-word-break', 'title']
        elif 'mercadolivre.' in domain or 'mercadolibre.' in domain or 'meli.la' in domain:
            selectors = ['.ui-pdp-title', 'title', '.page-title']
        else:
            selectors = ['meta[property="og:title"]', 'meta[name="title"]', 'title', 'h1', '.product-title', '.product-name']

        for selector in selectors:
            if selector.startswith('meta'):
                element = soup.select_one(selector)
                if element and element.get('content'):
                    return element.get('content').strip()
            else:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text(separator=' ', strip=True)
                    if len(text) > 5:
                        return text

        return None

    def _extract_amazon_description(self, soup):
        selectors = [
            '#feature-bullets',
            '#featurebullets_feature_div',
            '#productDescription',
            '.a-section.a-spacing-small.a-spacing-top-small',
        ]
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(separator=' ', strip=True)
                if len(text) > 20:
                    return text
        return None

    def _extract_mercado_livre_description(self, soup):
        selectors = [
            '.ui-pdp-description__content',
            '.ui-pdp-section__text',
            '.ui-pdp-content__description',
            '.ui-pdp-product-description',
            'meta[name="description"]',
        ]
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    text = element.get('content', '')
                else:
                    text = element.get_text(separator=' ', strip=True)
                if len(text) > 20:
                    return text
        return None

    def _clean_description(self, text, max_length):
        """Limpa e limita o tamanho da descrição."""
        # Remove quebras de linha e espaços extras
        text = re.sub(r'\s+', ' ', text.strip())

        # Remove caracteres especiais desnecessários
        text = re.sub(r'[^\w\s.,!?-À-ÿ]', '', text)

        # Limita o tamanho
        if len(text) > max_length:
            text = text[:max_length].rsplit(' ', 1)[0] + '...'

        return text