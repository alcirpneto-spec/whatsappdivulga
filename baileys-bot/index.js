const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
} = require("@whiskeysockets/baileys");
const pino = require("pino");
const qrcode = require("qrcode-terminal");
const { Pool } = require("pg");

const logger = pino({ level: process.env.LOG_LEVEL || "info" });

const DATABASE_URL = process.env.DATABASE_URL;
const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 60);
const MAX_LINKS_PER_CYCLE = Number(process.env.MAX_LINKS_PER_CYCLE || 10);
const SEND_INTERVAL_SECONDS = Number(process.env.SEND_INTERVAL_SECONDS || 300);
const QUEUE_REFRESH_INTERVAL_SECONDS = Number(
  process.env.QUEUE_REFRESH_INTERVAL_SECONDS || 3600
);
const BAILEYS_GROUP_JID = process.env.BAILEYS_GROUP_JID;
const WHATSAPP_GROUP_NAME = process.env.WHATSAPP_GROUP_NAME;

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL nao definido.");
}

const pool = new Pool({ connectionString: DATABASE_URL });

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function prettySource(source) {
  const value = String(source || "").replace(/_/g, " ").trim();
  if (!value) {
    return "Marketplace";
  }

  return value
    .split(/\s+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function cleanText(value, fallback = "") {
  if (!value) {
    return fallback;
  }

  return String(value).replace(/\s+/g, " ").trim();
}

function normalizeMetadata(value) {
  if (!value) {
    return {};
  }

  if (typeof value === "object") {
    return value;
  }

  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (error) {
      return {};
    }
  }

  return {};
}

function pickMeta(...candidates) {
  for (const candidate of candidates) {
    const value = cleanText(candidate);
    if (value) {
      return value;
    }
  }
  return "";
}

function formatCommission(value) {
  const raw = cleanText(value);
  if (!raw) {
    return "";
  }

  if (/%/.test(raw)) {
    return raw;
  }

  const numeric = Number.parseFloat(raw.replace(",", "."));
  if (!Number.isNaN(numeric)) {
    return `${numeric}%`;
  }

  return raw;
}

function formatDiscountPercent(value) {
  const raw = cleanText(value);
  if (!raw) {
    return "";
  }

  if (/%/.test(raw)) {
    return raw;
  }

  const numeric = Number.parseFloat(raw.replace(",", "."));
  if (Number.isNaN(numeric)) {
    return "";
  }

  return `${numeric.toFixed(0)}%`;
}

function parseSalesCount(value) {
  const raw = cleanText(value).toLowerCase();
  if (!raw) {
    return null;
  }

  if (raw.includes("mil")) {
    const base = raw.replace(/[^\d.,]/g, "").replace(/\./g, "").replace(",", ".");
    const numeric = Number.parseFloat(base);
    if (!Number.isNaN(numeric)) {
      return Math.round(numeric * 1000);
    }
  }

  const digits = raw.replace(/\D/g, "");
  if (!digits) {
    return null;
  }

  const numeric = Number.parseInt(digits, 10);
  return Number.isNaN(numeric) ? null : numeric;
}

function resolveOfferPricing(link, enrichment, metadata) {
  const currentPriceNumber =
    parsePriceNumber(enrichment.priceText) || parsePriceNumber(link.price_text);

  const originalPriceRaw = pickMeta(
    metadata.original_price,
    metadata.price_before,
    metadata.price_before_discount,
    metadata.list_price,
    metadata.old_price
  );

  const originalPriceNumber = parsePriceNumber(originalPriceRaw);
  const originalPriceText =
    originalPriceNumber !== null ? formatPriceFromNumber(originalPriceNumber) : "";

  let discountText = formatDiscountPercent(
    pickMeta(
      metadata.discount_pct,
      metadata.discount_percentage,
      metadata.discount
    )
  );

  if (!discountText && originalPriceNumber !== null && currentPriceNumber !== null && originalPriceNumber > currentPriceNumber) {
    const pct = ((originalPriceNumber - currentPriceNumber) / originalPriceNumber) * 100;
    discountText = `${pct.toFixed(0)}%`;
  }

  return {
    originalPriceText,
    discountText,
  };
}

function formatPrice(value) {
  const txt = cleanText(value);
  if (!txt) {
    return "";
  }

  if (/^R\$/i.test(txt)) {
    return txt;
  }

  return `R$ ${txt}`;
}

function parsePriceNumber(value) {
  const raw = cleanText(value);
  if (!raw) {
    return null;
  }

  const onlyDigits = raw.replace(/[^\d.,]/g, "");
  if (!onlyDigits) {
    return null;
  }

  let normalized = onlyDigits;
  const hasComma = normalized.includes(",");
  const hasDot = normalized.includes(".");

  if (hasComma && hasDot) {
    normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
  } else if (hasComma) {
    if (/,[0-9]{1,2}$/.test(normalized)) {
      normalized = normalized.replace(/,/g, ".");
    } else {
      normalized = normalized.replace(/,/g, "");
    }
  } else if (hasDot) {
    if (/\.[0-9]{1,2}$/.test(normalized)) {
      // keep as decimal separator
    } else {
      normalized = normalized.replace(/\./g, "");
    }
  }

  const numeric = Number.parseFloat(normalized);
  if (Number.isNaN(numeric) || numeric <= 0) {
    return null;
  }

  return numeric;
}

function formatPriceFromNumber(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 2,
  }).format(value);
}

function normalizePriceCandidate(value) {
  const numeric = parsePriceNumber(value);
  if (numeric === null) {
    return "";
  }

  return formatPriceFromNumber(numeric);
}

function extractBestPriceFromHtml(html) {
  const candidates = [];

  const brlRegex = /R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})|[0-9]+,[0-9]{2})/gi;
  for (const match of html.matchAll(brlRegex)) {
    const numeric = parsePriceNumber(match[1]);
    if (numeric !== null) {
      candidates.push(numeric);
    }
  }

  const jsonPriceRegex = /"(?:price|amount|sale_price|current_price|price_amount)"\s*:\s*"?([0-9]+(?:[.,][0-9]{1,2})?)"?/gi;
  for (const match of html.matchAll(jsonPriceRegex)) {
    const numeric = parsePriceNumber(match[1]);
    if (numeric !== null) {
      candidates.push(numeric);
    }
  }

  const filtered = candidates.filter((value) => value >= 10 && value <= 100000);
  if (filtered.length === 0) {
    return "";
  }

  // Choosing the highest reasonable value avoids installment values like 10x 39,90.
  const best = Math.max(...filtered);
  return formatPriceFromNumber(best);
}

function extractAndesAriaPrice(html) {
  const regex = /aria-label="(?:Agora:\s*)?([0-9.]+)\s+reais(?:\s+com\s+([0-9]{1,2})\s+centavos)?"/gi;

  for (const match of html.matchAll(regex)) {
    const reais = cleanText(match[1]).replace(/\./g, "");
    const cents = match[2] ? match[2].padStart(2, "0") : "00";
    const candidate = `${reais},${cents}`;
    const formatted = normalizePriceCandidate(candidate);
    if (formatted) {
      return formatted;
    }
  }

  return "";
}

function extractJsonLdOfferPrice(html) {
  const scripts = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>[\s\S]*?<\/script>/gi) || [];

  for (const scriptTag of scripts) {
    const contentMatch = scriptTag.match(/>([\s\S]*?)<\/script>/i);
    if (!contentMatch) {
      continue;
    }

    try {
      const parsed = JSON.parse(contentMatch[1]);
      const items = Array.isArray(parsed) ? parsed : [parsed];

      for (const item of items) {
        if (!item || typeof item !== "object") {
          continue;
        }

        const offers = item.offers || item.mainEntity?.offers;
        if (Array.isArray(offers) && offers.length > 0) {
          const candidate = normalizePriceCandidate(offers[0]?.price);
          if (candidate) {
            return candidate;
          }
        }

        if (offers && typeof offers === "object") {
          const candidate = normalizePriceCandidate(offers.price);
          if (candidate) {
            return candidate;
          }
        }
      }
    } catch (error) {
      // Ignore malformed JSON-LD and keep searching.
    }
  }

  return "";
}

function extractMercadoLivreItemId(value) {
  const text = cleanText(value).toUpperCase();
  if (!text) {
    return "";
  }

  const patterns = [/\b(ML[A-Z]-?\d{6,})\b/, /\b(MLB-?\d{6,})\b/];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return match[1].replace("-", "");
    }
  }

  return "";
}

function extractMercadoLivreSearchCode(value) {
  const text = cleanText(value).toUpperCase();
  if (!text) {
    return "";
  }

  const match = text.match(/\b([A-Z0-9]{4,}-[A-Z0-9]{4,})\b/);
  return match ? match[1] : "";
}

async function fetchMercadoLivreItemData(itemId) {
  if (!itemId) {
    return null;
  }

  try {
    const response = await fetch(`https://api.mercadolibre.com/items/${itemId}`, {
      method: "GET",
      headers: { accept: "application/json" },
    });

    if (!response.ok) {
      return null;
    }

    const data = await response.json();
    const firstPicture = Array.isArray(data.pictures) && data.pictures.length > 0 ? data.pictures[0] : null;

    // Some listings expose price in alternate fields instead of data.price.
    const priceFromItem =
      normalizePriceCandidate(data.price) ||
      normalizePriceCandidate(data.base_price) ||
      normalizePriceCandidate(data.original_price) ||
      normalizePriceCandidate(data.sale_price);

    let priceText = priceFromItem;

    if (!priceText) {
      const pricesResponse = await fetch(`https://api.mercadolibre.com/items/${itemId}/prices`, {
        method: "GET",
        headers: { accept: "application/json" },
      });

      if (pricesResponse.ok) {
        const pricesData = await pricesResponse.json();
        const prices = Array.isArray(pricesData.prices) ? pricesData.prices : [];

        const direct = prices.find((entry) => normalizePriceCandidate(entry.amount));
        const fromPromotion = prices.find(
          (entry) =>
            entry &&
            entry.conditions &&
            typeof entry.conditions === "object" &&
            normalizePriceCandidate(entry.amount)
        );

        const chosen = direct || fromPromotion;
        if (chosen) {
          priceText = normalizePriceCandidate(chosen.amount);
        }
      }
    }

    return {
      productName: cleanText(data.title),
      priceText,
      imageUrl: cleanText((firstPicture && (firstPicture.secure_url || firstPicture.url)) || data.thumbnail_secure_url || data.thumbnail),
    };
  } catch (error) {
    logger.warn({ err: error, itemId }, "Falha ao consultar API do Mercado Livre.");
    return null;
  }
}

async function fetchMercadoLivreSearchData(searchCode) {
  if (!searchCode) {
    return null;
  }

  try {
    const encoded = encodeURIComponent(searchCode);
    const response = await fetch(`https://api.mercadolibre.com/sites/MLB/search?q=${encoded}&limit=1`, {
      method: "GET",
      headers: { accept: "application/json" },
    });

    if (!response.ok) {
      return null;
    }

    const data = await response.json();
    const first = Array.isArray(data.results) && data.results.length > 0 ? data.results[0] : null;
    if (!first) {
      return null;
    }

    return {
      productName: cleanText(first.title),
      priceText: normalizePriceCandidate(first.price),
      imageUrl: cleanText(first.thumbnail_id ? `https://http2.mlstatic.com/D_NQ_NP_${first.thumbnail_id}-O.webp` : first.thumbnail),
    };
  } catch (error) {
    logger.warn({ err: error, searchCode }, "Falha ao consultar busca do Mercado Livre.");
    return null;
  }
}

function extractJsonLdProductName(html) {
  const scripts = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>[\s\S]*?<\/script>/gi) || [];

  for (const scriptTag of scripts) {
    const contentMatch = scriptTag.match(/>([\s\S]*?)<\/script>/i);
    if (!contentMatch) {
      continue;
    }

    try {
      const parsed = JSON.parse(contentMatch[1]);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const item of items) {
        if (item && typeof item === "object") {
          if (cleanText(item.name)) {
            return cleanText(item.name);
          }
          if (item.mainEntity && cleanText(item.mainEntity.name)) {
            return cleanText(item.mainEntity.name);
          }
        }
      }
    } catch (error) {
      // Ignore invalid JSON-LD blocks and keep trying other blocks.
    }
  }

  return "";
}

function extractMetaContent(html, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(
    `<meta[^>]+(?:property|name)=["']${escaped}["'][^>]+content=["']([^"']+)["'][^>]*>`,
    "i"
  );
  const match = html.match(regex);
  return match ? cleanText(match[1]) : "";
}

function extractRegexValue(html, regex) {
  const match = html.match(regex);
  return match ? cleanText(match[1]) : "";
}

function isGenericProductName(name) {
  const value = cleanText(name).toLowerCase();
  if (!value) {
    return true;
  }

  const genericNames = [
    "mercado livre",
    "perfil social",
    "social",
    "produto",
    "item",
  ];

  if (genericNames.includes(value)) {
    return true;
  }

  // Generic titles that include the marketplace name but no product context.
  if (value.startsWith("mercado livre") && value.length <= 24) {
    return true;
  }

  return false;
}

function pickBestProductName(candidates) {
  for (const candidate of candidates) {
    const value = cleanText(candidate);
    if (!isGenericProductName(value)) {
      return value;
    }
  }

  // Fallback to first non-empty value if all are generic.
  for (const candidate of candidates) {
    const value = cleanText(candidate);
    if (value) {
      return value;
    }
  }

  return "";
}

function extractCanonicalUrl(html) {
  return extractRegexValue(html, /<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i);
}

function extractFeaturedProductUrl(html) {
  // Prefer the highlighted main card of affiliate page.
  const listCardHref = extractRegexValue(
    html,
    /poly-card--list[\s\S]*?<a[^>]+href=["']([^"']+mercadolivre\.com\.br[^"']+)["']/i
  );
  if (listCardHref) {
    return listCardHref;
  }

  // Fallback: first product permalink from this page.
  const genericHref = extractRegexValue(html, /<a[^>]+href=["']([^"']+mercadolivre\.com\.br[^"']+)["'][^>]*class=["'][^"']*poly-component__title/i);
  return genericHref;
}

function inferSource(source, url) {
  const explicit = cleanText(source).toLowerCase();
  if (explicit) {
    return explicit;
  }

  const lowerUrl = cleanText(url).toLowerCase();
  if (/(^|\.)amazon\./i.test(lowerUrl) || lowerUrl.includes("amzn.to")) {
    return "amazon";
  }

  if (
    lowerUrl.includes("mercadolivre") ||
    lowerUrl.includes("mercadolibre") ||
    lowerUrl.includes("meli.la")
  ) {
    return "mercado_livre";
  }

  if (
    lowerUrl.includes("shopee") ||
    lowerUrl.includes("s.shopee") ||
    lowerUrl.includes("shp.ee")
  ) {
    return "shopee";
  }

  return "marketplace";
}

function extractAmazonPrice(html) {
  const selectors = [
    /id=["']priceblock_ourprice["'][^>]*>\s*([^<]+)</i,
    /id=["']priceblock_dealprice["'][^>]*>\s*([^<]+)</i,
    /id=["']priceblock_saleprice["'][^>]*>\s*([^<]+)</i,
    /class=["'][^"']*a-offscreen[^"']*["'][^>]*>\s*([^<]+)</i,
  ];

  for (const selector of selectors) {
    const raw = extractRegexValue(html, selector);
    const normalized = normalizePriceCandidate(raw);
    if (normalized) {
      return normalized;
    }
  }

  return "";
}

function extractShopeePrice(html) {
  const selectors = [
    /class=["'][^"']*pqTWkA[^"']*["'][^>]*>\s*([^<]+)</i,
    /class=["'][^"']*IZPeQz[^"']*["'][^>]*>\s*([^<]+)</i,
    /itemprop=["']price["'][^>]*content=["']([^"']+)["']/i,
  ];

  for (const selector of selectors) {
    const raw = extractRegexValue(html, selector);
    const normalized = normalizePriceCandidate(raw);
    if (normalized) {
      return normalized;
    }
  }

  return "";
}

async function fetchFeaturedProductData(productUrl) {
  if (!productUrl) {
    return null;
  }

  try {
    const response = await fetch(productUrl, {
      method: "GET",
      redirect: "follow",
      headers: {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
        accept: "text/html,application/xhtml+xml",
      },
    });

    if (!response.ok) {
      return null;
    }

    const html = await response.text();

    const priceFromMeta =
      normalizePriceCandidate(extractMetaContent(html, "product:price:amount")) ||
      normalizePriceCandidate(extractMetaContent(html, "og:price:amount")) ||
      extractJsonLdOfferPrice(html) ||
      extractAndesAriaPrice(html) ||
      extractBestPriceFromHtml(html);

    const titleFromMeta =
      extractMetaContent(html, "og:title") ||
      extractMetaContent(html, "twitter:title") ||
      extractRegexValue(html, /<title>([^<]+)<\/title>/i);

    const imageFromMeta =
      extractMetaContent(html, "og:image") ||
      extractMetaContent(html, "twitter:image") ||
      extractMetaContent(html, "twitter:image:src");

    return {
      priceText: priceFromMeta,
      productName: cleanText(titleFromMeta),
      imageUrl: cleanText(imageFromMeta),
    };
  } catch (error) {
    logger.warn({ err: error, productUrl }, "Falha ao consultar pagina do produto destacado.");
    return null;
  }
}

async function fetchEnrichment(link) {
  const fromDbPrice = cleanText(link.price_text);
  const fromDbImage = cleanText(link.image_url);

  if (fromDbPrice && fromDbImage) {
    return {
      priceText: normalizePriceCandidate(fromDbPrice) || formatPrice(fromDbPrice),
      imageUrl: fromDbImage,
      productName: cleanText(link.product_name),
      metadata: {},
    };
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);

    const response = await fetch(link.affiliate_url, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
        accept: "text/html,application/xhtml+xml",
      },
    });

    clearTimeout(timeout);

    if (!response.ok) {
      return {
        priceText: formatPrice(fromDbPrice),
        imageUrl: fromDbImage,
        metadata: {},
      };
    }

    const html = await response.text();
    const resolvedUrl = cleanText(response.url || link.affiliate_url);

    const ogUrl = extractMetaContent(html, "og:url");
    const canonicalUrl = extractCanonicalUrl(html);
    const featuredProductUrl = extractFeaturedProductUrl(html);

    const metaPrice =
      extractMetaContent(html, "product:price:amount") ||
      extractMetaContent(html, "og:price:amount") ||
      extractMetaContent(html, "twitter:data1");

    const metaDescription =
      extractMetaContent(html, "og:description") ||
      extractMetaContent(html, "description") ||
      extractMetaContent(html, "twitter:description");
    const metaDescriptionPrice = normalizePriceCandidate(metaDescription);

    const jsonLdOfferPrice = extractJsonLdOfferPrice(html);
    const andesAriaPrice = extractAndesAriaPrice(html);
    const htmlBestPrice = extractBestPriceFromHtml(html);

    const metaTitle =
      extractMetaContent(html, "og:title") ||
      extractMetaContent(html, "twitter:title") ||
      extractMetaContent(html, "title");

    const jsonLdName = extractJsonLdProductName(html);

    const metaImage =
      extractMetaContent(html, "og:image") ||
      extractMetaContent(html, "twitter:image") ||
      extractMetaContent(html, "twitter:image:src");

    const source = inferSource(link.source, resolvedUrl || link.affiliate_url);
    const isMercadoLivre = source === "mercado_livre";
    const isAmazon = source === "amazon";
    const isShopee = source === "shopee";

    let itemId = "";
    let searchCode = "";
    let primaryData = null;
    let featuredData = null;

    if (isMercadoLivre) {
      itemId =
        extractMercadoLivreItemId(link.affiliate_url) ||
        extractMercadoLivreItemId(resolvedUrl) ||
        extractMercadoLivreItemId(ogUrl) ||
        extractMercadoLivreItemId(canonicalUrl) ||
        extractMercadoLivreItemId(metaImage) ||
        extractMercadoLivreItemId(html);
      const itemData = await fetchMercadoLivreItemData(itemId);

      searchCode =
        extractMercadoLivreSearchCode(link.affiliate_url) ||
        extractMercadoLivreSearchCode(resolvedUrl) ||
        extractMercadoLivreSearchCode(link.product_name) ||
        extractMercadoLivreSearchCode(html);
      const searchData = itemData ? null : await fetchMercadoLivreSearchData(searchCode);
      primaryData = itemData || searchData;
      featuredData = await fetchFeaturedProductData(featuredProductUrl);
    }

    const sourceSpecificPrice =
      (isAmazon && extractAmazonPrice(html)) ||
      (isShopee && extractShopeePrice(html)) ||
      "";

    const finalPrice =
      normalizePriceCandidate(fromDbPrice) ||
      primaryData?.priceText ||
      featuredData?.priceText ||
      sourceSpecificPrice ||
      normalizePriceCandidate(metaPrice) ||
      metaDescriptionPrice ||
      jsonLdOfferPrice ||
      andesAriaPrice ||
      htmlBestPrice;

    const finalProductName = pickBestProductName([
      primaryData?.productName,
      featuredData?.productName,
      jsonLdName,
      metaTitle,
      cleanText(link.product_name),
    ]);

    if (!finalPrice) {
      logger.warn({ linkId: link.id, source, itemId, searchCode }, "Preco nao encontrado no enrichment desse link.");
    }

    return {
      priceText: finalPrice || formatPrice(fromDbPrice || metaPrice),
      imageUrl: cleanText(fromDbImage || primaryData?.imageUrl || featuredData?.imageUrl || metaImage),
      productName: finalProductName,
      metadata: {
        source,
        resolved_url: resolvedUrl,
      },
    };
  } catch (error) {
    logger.warn({ err: error, linkId: link.id }, "Falha ao enriquecer dados do link. Vou enviar sem imagem/preco se necessario.");
    return {
      priceText: normalizePriceCandidate(fromDbPrice) || formatPrice(fromDbPrice),
      imageUrl: fromDbImage,
      productName: cleanText(link.product_name),
      metadata: {},
    };
  }
}

async function persistEnrichment(id, enrichment) {
  const metadata = normalizeMetadata(enrichment.metadata);

  await pool.query(
    `
      UPDATE affiliate_links
      SET price_text = COALESCE(NULLIF($2, ''), price_text),
          image_url = COALESCE(NULLIF($3, ''), image_url),
          product_name = COALESCE(NULLIF($4, ''), product_name),
          metadata_json = COALESCE(metadata_json, '{}'::jsonb) || $5::jsonb
      WHERE id = $1
    `,
    [id, enrichment.priceText || "", enrichment.imageUrl || "", enrichment.productName || "", JSON.stringify(metadata)]
  );
}

async function ensureSchema() {
  const query = `
    CREATE TABLE IF NOT EXISTS affiliate_links (
      id BIGSERIAL PRIMARY KEY,
      product_name TEXT NOT NULL,
      affiliate_url TEXT NOT NULL,
      source TEXT DEFAULT 'mercado_livre',
      price_text TEXT,
      image_url TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      processed BOOLEAN NOT NULL DEFAULT FALSE,
      sent_at TIMESTAMPTZ,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT
    );

    ALTER TABLE affiliate_links ADD COLUMN IF NOT EXISTS price_text TEXT;
    ALTER TABLE affiliate_links ADD COLUMN IF NOT EXISTS image_url TEXT;
    ALTER TABLE affiliate_links ADD COLUMN IF NOT EXISTS metadata_json JSONB;

    CREATE INDEX IF NOT EXISTS idx_affiliate_links_pending
      ON affiliate_links (processed, created_at);

    CREATE INDEX IF NOT EXISTS idx_affiliate_links_shopee_itemid_created
      ON affiliate_links ((metadata_json->>'item_id'), created_at)
      WHERE source = 'shopee';

    CREATE INDEX IF NOT EXISTS idx_affiliate_links_shopee_canonical_url_created
      ON affiliate_links ((lower(rtrim(split_part(affiliate_url, '?', 1), '/'))), created_at)
      WHERE source = 'shopee';
  `;

  await pool.query(query);
  logger.info("Schema do banco validado.");
}

async function markAsSent(id) {
  await pool.query(
    `
      UPDATE affiliate_links
      SET processed = TRUE,
          sent_at = NOW(),
          last_error = NULL,
          attempts = attempts + 1
      WHERE id = $1
    `,
    [id]
  );
}

async function markAsFailed(id, errorMessage) {
  await pool.query(
    `
      UPDATE affiliate_links
      SET attempts = attempts + 1,
          last_error = $2
      WHERE id = $1
    `,
    [id, errorMessage]
  );
}

async function getPendingLinks(limit) {
  const result = await pool.query(
    `
      SELECT id, product_name, affiliate_url, source, price_text, image_url, created_at, attempts
         , COALESCE(metadata_json, '{}'::jsonb) AS metadata_json
      FROM affiliate_links
      WHERE processed = FALSE
      ORDER BY
        CASE
          WHEN lower(COALESCE(metadata_json->>'manual_priority', 'false')) IN ('true', '1', 'yes', 'on') THEN 0
          ELSE 1
        END,
        created_at ASC
      LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

async function resolveGroupJid(sock) {
  if (BAILEYS_GROUP_JID) {
    return BAILEYS_GROUP_JID;
  }

  if (!WHATSAPP_GROUP_NAME) {
    throw new Error(
      "Defina BAILEYS_GROUP_JID ou WHATSAPP_GROUP_NAME para identificar o grupo."
    );
  }

  const groups = await sock.groupFetchAllParticipating();
  const targetName = WHATSAPP_GROUP_NAME.trim().toLowerCase();

  const group = Object.values(groups).find((entry) => {
    return String(entry.subject || "").trim().toLowerCase() === targetName;
  });

  if (!group) {
    throw new Error(`Grupo '${WHATSAPP_GROUP_NAME}' nao encontrado na conta conectada.`);
  }

  return group.id;
}

function isRateOverlimitError(error) {
  const message = cleanText(error?.message).toLowerCase();
  const innerMessage = cleanText(error?.data).toLowerCase();
  const statusCode = error?.output?.statusCode;

  return (
    message.includes("rate-overlimit") ||
    message.includes("429") ||
    innerMessage.includes("429") ||
    statusCode === 429
  );
}

const lastTemplateIndexByGroup = new Map();

function normalizeForMatch(value) {
  return cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

const MESSAGE_TEMPLATE_GROUPS = {
  moda_feminina: {
    keywords: [
      "legging",
      "leg",
      "short feminino",
      "shorts feminino",
      "pantalona",
      "saia",
      "vestido",
      "blusa feminina",
      "alfaiataria feminina",
      "jogger feminina",
      "calca feminina",
      "calca canelada",
      "meia calca",
    ],
    templates: [
      { headline: "🔥 *Look novo com preco de oportunidade*", hook: "Conforto e estilo para usar hoje." },
      { headline: "✨ *Achadinho feminino do dia*", hook: "Peca versatil que combina com tudo." },
      { headline: "💃 *Seu look merece esse destaque*", hook: "Modelo em alta com otimo custo-beneficio." },
      { headline: "🛍️ *Oferta feminina selecionada pra voce*", hook: "Confortavel para trabalho, passeio e rotina." },
      { headline: "💥 *Calca queridinha em promocao*", hook: "Ideal para montar producoes lindas gastando menos." },
    ],
  },
  pijama_babydoll: {
    keywords: ["baby doll", "babydoll", "short doll", "pijama", "pijamas"],
    templates: [
      { headline: "🌙 *Conforto para noites mais leves*", hook: "Pijama lindo e pratico para o dia a dia." },
      { headline: "🛌 *Oferta especial de pijama*", hook: "Modelinho fresquinho e confortavel para descansar." },
      { headline: "✨ *Seu momento de descanso com estilo*", hook: "Escolha certeira para dormir melhor." },
      { headline: "💖 *Achado de baby doll em destaque*", hook: "Otimo preco para renovar seu kit de noite." },
      { headline: "🔥 *Pijama feminino com precinho bom*", hook: "Confortavel, bonito e pronto para usar." },
    ],
  },
  moda_masculina: {
    keywords: [
      "bermuda",
      "camisa polo",
      "camiseta",
      "jogger masculina",
      "calca sarja",
      "jaqueta masculina",
      "shorts masculino",
      "calca masculina",
    ],
    templates: [
      { headline: "👔 *Estilo masculino com preco justo*", hook: "Visual alinhado para todas as ocasioes." },
      { headline: "🔥 *Peca masculina em oferta hoje*", hook: "Conforto e qualidade para o dia a dia." },
      { headline: "💼 *Achado masculino selecionado*", hook: "Modelo pratico para trabalho e lazer." },
      { headline: "⚡ *Oportunidade para renovar o guarda-roupa*", hook: "Item versatil com excelente custo-beneficio." },
      { headline: "🛍️ *Oferta masculina que vale o clique*", hook: "Estilo e conforto no mesmo produto." },
    ],
  },
  audio_fones: {
    keywords: ["fone", "bluetooth", "tws", "earbud", "caixa de som", "som estereo"],
    templates: [
      { headline: "🎧 *Som sem fio com preco de achado*", hook: "Ideal para treino, trabalho e rotina." },
      { headline: "🔊 *Audio em destaque hoje*", hook: "Modelo queridinho com otimo custo-beneficio." },
      { headline: "⚡ *Upgrade no som sem gastar muito*", hook: "Conexao pratica e uso no dia inteiro." },
      { headline: "🔥 *Fone bluetooth em promocao*", hook: "Boa opcao para quem curte praticidade." },
      { headline: "🎵 *Achadinho tech para ouvir melhor*", hook: "Produto versatil para todas as horas." },
    ],
  },
  cozinha_utilidades: {
    keywords: [
      "marmita",
      "pote",
      "fatiador",
      "cozinha",
      "panela",
      "chaleira",
      "escorredor",
      "porta temperos",
      "faqueiro",
      "tacas",
      "xicara",
      "coador",
      "torneira",
      "utensilios",
    ],
    templates: [
      { headline: "🍳 *Cozinha mais pratica com esse achado*", hook: "Utilidade que facilita sua rotina." },
      { headline: "🏠 *Item de casa em oferta hoje*", hook: "Organizacao e praticidade em um so produto." },
      { headline: "✨ *Achadinho util para o dia a dia*", hook: "Perfeito para quem ama praticidade." },
      { headline: "🛒 *Utilidade de cozinha que compensa*", hook: "Produto funcional com preco atraente." },
      { headline: "🔥 *Oferta selecionada para sua cozinha*", hook: "Mais eficiencia e conforto na rotina." },
    ],
  },
  ferramentas_reparo: {
    keywords: [
      "broca",
      "parafusadeira",
      "catraca",
      "ferramenta",
      "pistola",
      "veda",
      "impermeabil",
      "manta liquida",
      "tinta",
      "verniz",
      "spray",
      "chave",
      "alicate",
      "pulverizador",
    ],
    templates: [
      { headline: "🛠️ *Ferramenta util com preco de oportunidade*", hook: "Boa pedida para reparos e manutencao." },
      { headline: "⚙️ *Achado para quem resolve tudo em casa*", hook: "Pratico para obra, ajuste e melhoria." },
      { headline: "🔥 *Produto de reparo em destaque*", hook: "Custo-beneficio forte para rotina de casa." },
      { headline: "🏗️ *Item de manutencao que vale ter*", hook: "Mais praticidade para o seu dia a dia." },
      { headline: "💥 *Oferta de ferramenta selecionada*", hook: "Produto funcional com excelente valor." },
    ],
  },
  casa_enxoval: {
    keywords: [
      "lencol",
      "lençol",
      "coberdrom",
      "cobertor",
      "travesseiro",
      "toalha",
      "capa protetora colchao",
      "capa de colchao",
      "enxoval",
      "mesa de cabeceira",
      "quadro decorativo",
      "garrafa termica",
    ],
    templates: [
      { headline: "🛏️ *Conforto para sua casa em oferta*", hook: "Item de enxoval e casa com preco bom." },
      { headline: "🏡 *Achadinho para deixar o lar mais completo*", hook: "Praticidade e conforto na medida certa." },
      { headline: "✨ *Produto de casa em destaque hoje*", hook: "Excelente opcao para renovar ambientes." },
      { headline: "🧺 *Oferta de casa e enxoval selecionada*", hook: "Mais conforto para sua rotina." },
      { headline: "🔥 *Item para casa com otimo custo-beneficio*", hook: "Vale conferir enquanto esta nesse valor." },
    ],
  },
  beleza_cuidados: {
    keywords: [
      "body splash",
      "mascara",
      "cabelo",
      "anti idade",
      "anti-idade",
      "massageador",
      "perfume",
      "attraction",
      "hidratante",
      "botox",
      "hyaluron",
      "beleza",
      "seringa",
    ],
    templates: [
      { headline: "💄 *Cuidado pessoal com preco de oportunidade*", hook: "Produto de beleza para sua rotina." },
      { headline: "✨ *Seu autocuidado em destaque hoje*", hook: "Achadinho para se cuidar gastando menos." },
      { headline: "🌸 *Oferta de beleza selecionada*", hook: "Boa opcao para manter sua rotina em dia." },
      { headline: "🔥 *Produto de cuidados em promocao*", hook: "Mais praticidade no seu momento de beleza." },
      { headline: "🧴 *Achadinho de beleza que vale o clique*", hook: "Produto util com excelente custo-beneficio." },
    ],
  },
  fitness_saude: {
    keywords: [
      "creatina",
      "fitness",
      "academia",
      "bioimpedancia",
      "mini band",
      "treino",
      "esportivo",
      "caminhada",
      "fisioterapia",
      "suplement",
    ],
    templates: [
      { headline: "💪 *Oferta fitness para sua evolucao*", hook: "Item funcional para treino e performance." },
      { headline: "🏋️ *Achadinho para rotina de treino*", hook: "Mais resultado com melhor custo-beneficio." },
      { headline: "⚡ *Produto esportivo em destaque hoje*", hook: "Boa escolha para quem busca evoluir." },
      { headline: "🔥 *Item fitness com preco de oportunidade*", hook: "Perfeito para complementar seus treinos." },
      { headline: "🚀 *Oferta para quem leva treino a serio*", hook: "Produto pratico para sua rotina ativa." },
    ],
  },
  neutro: {
    keywords: [],
    templates: [
      { headline: "🔥 *Achado do dia para economizar*", hook: "Oferta atual com bom custo-beneficio." },
      { headline: "✨ *Oferta em destaque agora*", hook: "Item interessante para conferir no link." },
      { headline: "🛒 *Preco bom em produto util*", hook: "Selecionado para quem gosta de oportunidade." },
      { headline: "⚡ *Oferta com valor competitivo*", hook: "Vale verificar enquanto esta disponivel." },
      { headline: "📌 *Dica de compra do momento*", hook: "Opcao com preco atrativo para hoje." },
    ],
  },
};

const NEUTRAL_TOPIC_RULES = [
  {
    topic: "moda",
    keywords: ["short", "shorts", "calca", "camisa", "camiseta", "blusa", "saia", "vestido", "legging", "bermuda", "polo", "jogger"],
    templates: [
      { headline: "🛍️ *Achado de moda com preco baixo*", hook: "Peca em destaque para renovar o guarda-roupa." },
      { headline: "✨ *Oferta de moda para aproveitar*", hook: "Item versatil com valor interessante hoje." },
      { headline: "🔥 *Moda em promocao no momento*", hook: "Boa oportunidade para comprar pagando menos." },
    ],
  },
  {
    topic: "beleza",
    keywords: ["perfume", "body splash", "hidratante", "cabelo", "mascara", "massageador", "botox", "hyaluron", "serum"],
    templates: [
      { headline: "💄 *Oferta de beleza em destaque*", hook: "Produto de cuidados com preco atrativo." },
      { headline: "🌸 *Achadinho de autocuidado hoje*", hook: "Item de beleza com custo-beneficio interessante." },
      { headline: "✨ *Preco bom para cuidar de voce*", hook: "Vale conferir essa oportunidade de beleza." },
    ],
  },
  {
    topic: "casa",
    keywords: ["cozinha", "panela", "marmita", "pote", "toalha", "lencol", "travesseiro", "utensilio", "escorredor", "torneira"],
    templates: [
      { headline: "🏠 *Utilidade para casa com preco bom*", hook: "Produto pratico para facilitar a rotina." },
      { headline: "🧺 *Oferta para o dia a dia da casa*", hook: "Item funcional com valor competitivo." },
      { headline: "🍳 *Achado util para sua rotina*", hook: "Boa opcao para organizar e simplificar tarefas." },
    ],
  },
  {
    topic: "tecnologia",
    keywords: ["fone", "bluetooth", "tws", "caixa de som", "earbud", "carregador", "smart", "gadget"],
    templates: [
      { headline: "🎧 *Oferta de tecnologia em destaque*", hook: "Produto pratico com preco interessante hoje." },
      { headline: "⚡ *Achado tech com bom valor*", hook: "Boa oportunidade para quem curte tecnologia." },
      { headline: "🔊 *Item de audio e tech para conferir*", hook: "Custo-beneficio atrativo no momento." },
    ],
  },
  {
    topic: "saude_fitness",
    keywords: ["creatina", "fitness", "treino", "academia", "suplement", "fisioterapia", "bioimpedancia"],
    templates: [
      { headline: "💪 *Oferta fitness com preco competitivo*", hook: "Item em destaque para sua rotina de treino." },
      { headline: "🏋️ *Achado esportivo do momento*", hook: "Produto util para quem busca performance." },
      { headline: "🚀 *Preco bom para evoluir no treino*", hook: "Vale conferir essa opcao fitness." },
    ],
  },
];

const CATEGORY_GROUP_HINTS = {
  100017: "moda_feminina",
  100011: "moda_masculina",
  100535: "audio_fones",
  100630: "beleza_cuidados",
  100001: "fitness_saude",
};

function resolveCategoryHintGroup(metadata) {
  const categoryCandidates = [metadata.category_id, ...(Array.isArray(metadata.category_ids) ? metadata.category_ids : [])];

  for (const category of categoryCandidates) {
    const numeric = Number.parseInt(category, 10);
    if (!Number.isNaN(numeric) && CATEGORY_GROUP_HINTS[numeric]) {
      return CATEGORY_GROUP_HINTS[numeric];
    }
  }

  return "";
}

function resolveProductGroupKey(productName, metadata) {
  const normalizedName = normalizeForMatch(productName);

  for (const [groupKey, groupConfig] of Object.entries(MESSAGE_TEMPLATE_GROUPS)) {
    if (groupKey === "neutro") {
      continue;
    }

    if (groupConfig.keywords.some((keyword) => normalizedName.includes(normalizeForMatch(keyword)))) {
      return groupKey;
    }
  }

  const categoryGroup = resolveCategoryHintGroup(metadata);
  if (categoryGroup) {
    return categoryGroup;
  }

  return "neutro";
}

function pickTemplateForGroup(groupKey) {
  const groupConfig = MESSAGE_TEMPLATE_GROUPS[groupKey] || MESSAGE_TEMPLATE_GROUPS.neutro;
  const templates = groupConfig.templates;

  if (!Array.isArray(templates) || templates.length === 0) {
    return MESSAGE_TEMPLATE_GROUPS.neutro.templates[0];
  }

  const previousIndex = lastTemplateIndexByGroup.get(groupKey);
  const candidateIndexes = templates
    .map((_, index) => index)
    .filter((index) => index !== previousIndex);

  const randomPool = candidateIndexes.length > 0 ? candidateIndexes : [0];
  const selectedIndex = randomPool[Math.floor(Math.random() * randomPool.length)];
  lastTemplateIndexByGroup.set(groupKey, selectedIndex);

  return templates[selectedIndex];
}

function resolveNeutralTopicConfig(productName) {
  const normalizedName = normalizeForMatch(productName);

  for (const rule of NEUTRAL_TOPIC_RULES) {
    const matched = rule.keywords.some((keyword) => normalizedName.includes(normalizeForMatch(keyword)));
    if (matched) {
      return rule;
    }
  }

  return null;
}

function pickTemplateForNeutralTopic(productName) {
  const topicConfig = resolveNeutralTopicConfig(productName);
  const templates = topicConfig?.templates || MESSAGE_TEMPLATE_GROUPS.neutro.templates;
  const topicKey = topicConfig?.topic || "geral";
  const memoryKey = `neutro_${topicKey}`;

  const previousIndex = lastTemplateIndexByGroup.get(memoryKey);
  const candidateIndexes = templates
    .map((_, index) => index)
    .filter((index) => index !== previousIndex);

  const randomPool = candidateIndexes.length > 0 ? candidateIndexes : [0];
  const selectedIndex = randomPool[Math.floor(Math.random() * randomPool.length)];
  lastTemplateIndexByGroup.set(memoryKey, selectedIndex);

  return templates[selectedIndex];
}

function buildMessage(link, enrichment) {
  const sourceLabel = `*Fonte:* ${prettySource(inferSource(link.source, link.affiliate_url))}`;
  const priceLabel = enrichment.priceText ? `*Preço:* ${enrichment.priceText}` : "";
  const productName = cleanText(enrichment.productName || link.product_name || "Produto sem nome");
  const metadata = {
    ...normalizeMetadata(link.metadata_json),
    ...normalizeMetadata(enrichment.metadata),
  };

  const salesValue = pickMeta(metadata.sales, metadata.sold, metadata.historical_sold);
  const shopValue = pickMeta(metadata.shop_name, metadata.shopName, metadata.store_name);
  const pricing = resolveOfferPricing(link, enrichment, metadata);
  const originalPriceLabel = pricing.originalPriceText ? `*De:* ${pricing.originalPriceText}` : "";
  const discountLabel = pricing.discountText ? `*Desconto:* ${pricing.discountText}` : "";
  const salesCount = parseSalesCount(salesValue);
  const salesLabel = salesValue && salesCount !== null && salesCount >= 300 ? `*Vendas:* ${salesValue}` : "";
  const shopLabel = shopValue ? `*Loja:* ${shopValue}` : "";

  const groupKey = resolveProductGroupKey(productName, metadata);
  const selectedTemplate = groupKey === "neutro"
    ? pickTemplateForNeutralTopic(productName)
    : pickTemplateForGroup(groupKey);
  const hookLine = cleanText(selectedTemplate.hook);

  return [
    selectedTemplate.headline,
    hookLine,
    "",
    `*Produto:* ${productName}`,
    priceLabel,
    originalPriceLabel,
    discountLabel,
    salesLabel,
    shopLabel,
    sourceLabel,
    `Link: ${link.affiliate_url}`,
  ]
    .filter(Boolean)
    .join("\n");
}

async function startBaileysConnection() {
  const { state, saveCreds } = await useMultiFileAuthState("./auth");
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    logger: pino({ level: "silent" }),
  });

  let isReady = false;
  let shouldReconnect = true;
  let reconnectDelayMs = 0;

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      logger.info("QR recebido. Escaneie com o WhatsApp para autenticar.");
      qrcode.generate(qr, { small: true });
    }

    if (connection === "open") {
      isReady = true;
      logger.info("Conexao WhatsApp aberta com sucesso.");
    }

    if (connection === "close") {
      isReady = false;
      const statusCode =
        lastDisconnect &&
        lastDisconnect.error &&
        lastDisconnect.error.output &&
        lastDisconnect.error.output.statusCode;

      if (statusCode === DisconnectReason.loggedOut) {
        shouldReconnect = false;
        reconnectDelayMs = 5000;
        logger.error("Sessao desconectada (loggedOut). Remova ./auth e autentique novamente.");
        return;
      }

      // Any close event needs a fresh socket instance. Keep the reason in logs and
      // break the current loop so run() can create a new connection object.
      shouldReconnect = false;
      reconnectDelayMs = statusCode === DisconnectReason.restartRequired ? 1000 : 5000;
      logger.warn({ statusCode }, "Conexao fechada. Vou recriar a sessao...");
    }
  });

  return {
    sock,
    isReady: () => isReady,
    shouldReconnect: () => shouldReconnect,
    reconnectDelayMs: () => reconnectDelayMs,
  };
}

async function run() {
  await ensureSchema();

  while (true) {
    const connection = await startBaileysConnection();
    let cachedGroupJid = "";
    let windowStartedAt = 0;
    let sentInWindow = 0;

    while (connection.shouldReconnect()) {
      if (!connection.isReady()) {
        await delay(2000);
        continue;
      }

      if (!cachedGroupJid) {
        try {
          cachedGroupJid = await resolveGroupJid(connection.sock);
          logger.info({ groupJid: cachedGroupJid }, "Grupo resolvido com sucesso.");
        } catch (error) {
          const waitSeconds = isRateOverlimitError(error)
            ? Math.max(POLL_INTERVAL_SECONDS, 120)
            : POLL_INTERVAL_SECONDS;

          logger.error(
            { err: error, waitSeconds },
            "Falha ao resolver grupo. Vou tentar novamente depois do intervalo."
          );
          await delay(waitSeconds * 1000);
          continue;
        }
      }

      const nowMs = Date.now();
      if (windowStartedAt === 0 || nowMs - windowStartedAt >= QUEUE_REFRESH_INTERVAL_SECONDS * 1000) {
        windowStartedAt = nowMs;
        sentInWindow = 0;

        logger.info(
          {
            queueRefreshIntervalSeconds: QUEUE_REFRESH_INTERVAL_SECONDS,
            sendIntervalSeconds: SEND_INTERVAL_SECONDS,
            maxLinksPerWindow: MAX_LINKS_PER_CYCLE,
          },
          "Nova janela de envio iniciada."
        );
      }

      if (sentInWindow >= MAX_LINKS_PER_CYCLE) {
        const remainingWindowMs = Math.max(1000, QUEUE_REFRESH_INTERVAL_SECONDS * 1000 - (nowMs - windowStartedAt));
        logger.info(
          {
            sentInWindow,
            maxLinksPerWindow: MAX_LINKS_PER_CYCLE,
            waitSeconds: Math.ceil(remainingWindowMs / 1000),
          },
          "Limite de envios da janela atingido. Aguardando proxima janela."
        );
        await delay(remainingWindowMs);
        continue;
      }

      const batch = await getPendingLinks(1);
      if (batch.length === 0) {
        await delay(POLL_INTERVAL_SECONDS * 1000);
        continue;
      }

      const link = batch[0];

      try {
        const enrichment = await fetchEnrichment(link);
        await persistEnrichment(link.id, enrichment);

        const message = buildMessage(link, enrichment);

        if (enrichment.imageUrl) {
          try {
            await connection.sock.sendMessage(cachedGroupJid, {
              image: { url: enrichment.imageUrl },
              caption: message,
            });
          } catch (imageError) {
            logger.warn({ err: imageError, linkId: link.id }, "Falha ao enviar imagem. Vou enviar texto.");
            await connection.sock.sendMessage(cachedGroupJid, { text: message });
          }
        } else {
          await connection.sock.sendMessage(cachedGroupJid, { text: message });
        }

        await markAsSent(link.id);
        sentInWindow += 1;
        logger.info(
          {
            linkId: link.id,
            sentInWindow,
            maxLinksPerWindow: MAX_LINKS_PER_CYCLE,
            nextSendInSeconds: SEND_INTERVAL_SECONDS,
          },
          "Link enviado com sucesso."
        );
      } catch (error) {
        const errorMessage = error && error.message ? error.message.slice(0, 500) : "Erro desconhecido";
        await markAsFailed(link.id, errorMessage);
        logger.error({ err: error, linkId: link.id }, "Falha ao enviar link.");
      }

      const nextBatch = await getPendingLinks(1);
      const nextIsManual =
        nextBatch.length > 0 &&
        normalizeMetadata(nextBatch[0].metadata_json).manual_priority === true;

      if (nextIsManual) {
        logger.info({ linkId: nextBatch[0].id }, "Proximo item e manual_priority. Pulando intervalo de envio.");
        await delay(3000);
      } else {
        await delay(SEND_INTERVAL_SECONDS * 1000);
      }
    }

    const waitMs = connection.reconnectDelayMs();
    logger.warn(`Reconectando sessao em ${waitMs}ms...`);
    await delay(waitMs);
  }
}

run().catch((error) => {
  logger.error({ err: error }, "Erro fatal no worker Baileys.");
  process.exit(1);
});
