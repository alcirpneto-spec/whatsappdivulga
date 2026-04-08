import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

SALES_DATA_PATH = DATA_DIR / "sales_data.csv"
LINKS_PATH = DATA_DIR / "products_links.md"
NEW_LINKS_PATH = DATA_DIR / "new_links.md"

# Configurações da API Shopee Affiliate
SHOPEE_APP_ID = os.getenv("SHOPEE_APP_ID", "your_app_id_here")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "your_app_secret_here")
SHOPEE_AFFILIATE_ID = os.getenv("SHOPEE_AFFILIATE_ID", "your_affiliate_id_here")

GROUP_NAME = os.getenv("GROUP_NAME", "Nome do Grupo")  # Substitua pelo nome exato do grupo do WhatsApp
CHROME_DRIVER_PATH = os.getenv("CHROME_DRIVER_PATH", r"C:\path\to\chromedriver.exe")  # Atualize para o caminho do seu ChromeDriver
CHROME_BINARY_PATH = os.getenv("CHROME_BINARY_PATH", None)
WHATSAPP_PROFILE_DIR = os.getenv("WHATSAPP_PROFILE_DIR", str(BASE_DIR / "whatsapp-profile"))
CHECK_TIME = os.getenv("CHECK_TIME", "09:00")  # Horário diário de verificação
TOP_K = int(os.getenv("TOP_K", "2"))  # Quantos produtos priorizar
MAX_MESSAGE_ITEMS = int(os.getenv("MAX_MESSAGE_ITEMS", "3"))
USE_WHATSAPP_AUTOMATION = os.getenv("USE_WHATSAPP_AUTOMATION", "True").lower() in ("1", "true", "yes")  # False apenas para testar sem enviar
USE_SHOPEE_API = os.getenv("USE_SHOPEE_API", "False").lower() in ("1", "true", "yes")  # True para usar API da Shopee em vez de links manuais
