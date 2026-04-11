import os
from urllib.parse import urlparse

import psycopg2
from flask import Flask, jsonify, render_template, request


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder="../web/templates",
        static_folder="../web/static",
    )

    database_url = os.getenv("DATABASE_URL", "").strip()

    def detect_source(url: str) -> str:
        host = (urlparse(url).netloc or "").lower()
        if "amazon." in host:
            return "amazon"
        if "shopee." in host:
            return "shopee"
        if "mercadolivre." in host or "meli.la" in host:
            return "mercado_livre"
        return "mercado_livre"

    def get_connection():
        if not database_url:
            raise RuntimeError("DATABASE_URL nao configurada")
        return psycopg2.connect(database_url)

    @app.get("/health")
    def health():
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return jsonify({"status": "ok"}), 200
        except Exception as exc:
            return jsonify({"status": "error", "message": str(exc)}), 500

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/recent")
    def list_recent_links():
        limit = 8
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, product_name, affiliate_url, source, created_at
                        FROM affiliate_links
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()

            items = [
                {
                    "id": row[0],
                    "product_name": row[1],
                    "affiliate_url": row[2],
                    "source": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                }
                for row in rows
            ]
            return jsonify({"items": items}), 200
        except Exception as exc:
            return jsonify({"items": [], "message": str(exc)}), 500

    @app.post("/api/affiliate-links")
    def create_affiliate_link():
        payload = request.get_json(silent=True) or {}
        product_name = (payload.get("product_name") or "").strip()
        affiliate_url = (payload.get("affiliate_url") or "").strip()
        source = (payload.get("source") or "auto").strip().lower()

        if not product_name:
            return jsonify({"ok": False, "message": "Informe o codigo/id do produto."}), 400
        if not affiliate_url:
            return jsonify({"ok": False, "message": "Informe a URL de afiliado."}), 400
        if not (affiliate_url.startswith("http://") or affiliate_url.startswith("https://")):
            return jsonify({"ok": False, "message": "URL invalida. Use http:// ou https://."}), 400

        if source == "auto":
            source = detect_source(affiliate_url)

        allowed_sources = {"mercado_livre", "amazon", "shopee"}
        if source not in allowed_sources:
            return jsonify({"ok": False, "message": "Fonte invalida."}), 400

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO affiliate_links (product_name, affiliate_url, source)
                        VALUES (%s, %s, %s)
                        RETURNING id, created_at
                        """,
                        (product_name, affiliate_url, source),
                    )
                    row = cur.fetchone()

            return (
                jsonify(
                    {
                        "ok": True,
                        "message": "Link cadastrado com sucesso.",
                        "item": {
                            "id": row[0],
                            "created_at": row[1].isoformat() if row and row[1] else None,
                            "product_name": product_name,
                            "affiliate_url": affiliate_url,
                            "source": source,
                        },
                    }
                ),
                201,
            )
        except Exception as exc:
            return jsonify({"ok": False, "message": f"Erro ao salvar: {exc}"}), 500

    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("WEB_PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
