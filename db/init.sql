CREATE TABLE IF NOT EXISTS affiliate_links (
    id BIGSERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    affiliate_url TEXT NOT NULL,
    source TEXT DEFAULT 'mercado_livre',
    price_text TEXT,
    image_url TEXT,
    metadata_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    sent_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_affiliate_links_pending
    ON affiliate_links (processed, created_at);

CREATE INDEX IF NOT EXISTS idx_affiliate_links_shopee_itemid_created
    ON affiliate_links ((metadata_json->>'item_id'), created_at)
    WHERE source = 'shopee';

CREATE INDEX IF NOT EXISTS idx_affiliate_links_shopee_canonical_url_created
    ON affiliate_links ((lower(rtrim(split_part(affiliate_url, '?', 1), '/'))), created_at)
    WHERE source = 'shopee';
