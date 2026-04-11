import json
import os
from datetime import datetime, timedelta, timezone
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

    def ensure_worker_runs_table():
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_runs (
                        id BIGSERIAL PRIMARY KEY,
                        worker_name TEXT NOT NULL,
                        run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        status TEXT NOT NULL,
                        inserted_count INTEGER NOT NULL DEFAULT 0,
                        skipped_count INTEGER NOT NULL DEFAULT 0,
                        error_message TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_worker_runs_worker_name_run_at
                        ON worker_runs (worker_name, run_at DESC)
                    """
                )

    ensure_worker_runs_table()

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
            manual_metadata = {
                "manual_priority": True,
                "created_via": "front_web",
            }

            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO affiliate_links (product_name, affiliate_url, source, metadata_json)
                        VALUES (%s, %s, %s, %s::jsonb)
                        RETURNING id, created_at
                        """,
                        (product_name, affiliate_url, source, json.dumps(manual_metadata)),
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

    @app.get("/api/dashboard")
    def dashboard_status():
        try:
            use_shopee_api = get_env_bool("USE_SHOPEE_API", True)
            interval_minutes = max(1, get_env_int("SCHEDULE_INTERVAL_MINUTES", 30))

            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT run_at, status, inserted_count, skipped_count, error_message
                        FROM worker_runs
                        WHERE worker_name = 'shopee_discovery'
                        ORDER BY run_at DESC
                        LIMIT 1
                        """
                    )
                    run_row = cur.fetchone()

                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM affiliate_links
                        WHERE processed = FALSE
                        """
                    )
                    pending_total = cur.fetchone()[0]

                    cur.execute(
                        """
                        SELECT source, COUNT(*)
                        FROM affiliate_links
                        WHERE processed = FALSE
                        GROUP BY source
                        ORDER BY COUNT(*) DESC
                        """
                    )
                    pending_by_source_rows = cur.fetchall()

                    cur.execute(
                        """
                        SELECT id, product_name, source, created_at
                        FROM affiliate_links
                        WHERE processed = FALSE
                        ORDER BY created_at ASC
                        LIMIT 10
                        """
                    )
                    pending_items_rows = cur.fetchall()

            now = datetime.now(timezone.utc)
            last_run_at = run_row[0] if run_row else None
            next_run_at = (last_run_at + timedelta(minutes=interval_minutes)) if last_run_at else None
            seconds_until_next_run = 0
            if next_run_at:
                seconds_until_next_run = max(0, int((next_run_at - now).total_seconds()))

            pending_by_source = {
                (row[0] or "desconhecido"): row[1]
                for row in pending_by_source_rows
            }

            pending_items = [
                {
                    "id": row[0],
                    "product_name": row[1],
                    "source": row[2],
                    "created_at": row[3].isoformat() if row[3] else None,
                }
                for row in pending_items_rows
            ]

            shopee_run = {
                "enabled": use_shopee_api,
                "interval_minutes": interval_minutes,
                "last_run_at": last_run_at.isoformat() if last_run_at else None,
                "next_run_at": next_run_at.isoformat() if next_run_at else None,
                "seconds_until_next_run": seconds_until_next_run,
                "status": run_row[1] if run_row else "unknown",
                "inserted_count": run_row[2] if run_row else 0,
                "skipped_count": run_row[3] if run_row else 0,
                "error_message": run_row[4] if run_row else "",
            }

            queue = {
                "pending_total": pending_total,
                "pending_by_source": pending_by_source,
                "pending_items": pending_items,
                "refresh_seconds": 300,
            }

            return jsonify({"shopee_discovery": shopee_run, "queue": queue}), 200
        except Exception as exc:
            return jsonify({"message": str(exc)}), 500

    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("WEB_PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
