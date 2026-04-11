import json
import logging

from config import SHOPEE_APP_ID, SHOPEE_AFFILIATE_ID, USE_SHOPEE_API
from services.shopee_api import ShopeeAffiliateAPI


def mask(value: str) -> str:
    if not value:
        return "(vazio)"
    if len(value) <= 4:
        return "****"
    return f"{value[:2]}***{value[-2:]}"


def run_test(keyword: str = "carregador", limit: int = 3) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("=== TESTE API SHOPEE ===")
    print(f"USE_SHOPEE_API: {USE_SHOPEE_API}")
    print(f"SHOPEE_APP_ID: {mask(SHOPEE_APP_ID)}")
    print(f"SHOPEE_AFFILIATE_ID: {mask(SHOPEE_AFFILIATE_ID)}")

    api = ShopeeAffiliateAPI()

    print("\n1) Handshake GraphQL (__typename)")
    ping = api._execute_graphql("query { __typename }")
    print(json.dumps(ping, ensure_ascii=False, indent=2))

    print(f"\n2) Buscando produtos: keyword='{keyword}', limit={limit}")
    products = api.search_products(keyword=keyword, limit=limit)

    print(f"Total retornado: {len(products)}")

    if products:
        print("\nPrimeiro item retornado:")
        print(json.dumps(products[0], ensure_ascii=False, indent=2))
        return 0

    print("\nNenhum produto retornado.")
    print("Verifique credenciais, affiliate_id e os campos/query permitidos no schema da sua conta.")
    return 1


if __name__ == "__main__":
    raise SystemExit(run_test())
