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
const AI_COPY_ENABLED = ["1", "true", "yes", "on"].includes(
  String(process.env.AI_COPY_ENABLED || "false").trim().toLowerCase()
);
const AI_COPY_API_KEY = String(process.env.AI_COPY_API_KEY || "").trim();
const AI_COPY_MODEL = String(process.env.AI_COPY_MODEL || "gpt-4o-mini").trim();
const AI_COPY_API_URL = String(process.env.AI_COPY_API_URL || "https://api.openai.com/v1/chat/completions").trim();
const AI_COPY_TIMEOUT_MS = Number(process.env.AI_COPY_TIMEOUT_MS || 12000);
const ACTIVE_START_HOUR = Number.parseInt(
  String(process.env.ACTIVE_START_HOUR || "").trim(),
  10
);
const ACTIVE_END_HOUR = Number.parseInt(
  String(process.env.ACTIVE_END_HOUR || "").trim(),
  10
);
const ACTIVE_TIMEZONE = String(process.env.ACTIVE_TIMEZONE || "America/Sao_Paulo").trim();

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL nao definido.");
}

const pool = new Pool({ connectionString: DATABASE_URL });

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isValidHour(value) {
  return Number.isInteger(value) && value >= 0 && value <= 23;
}

function getHourInTimezone(date, timeZone) {
  const formatted = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    hour12: false,
    timeZone,
  }).format(date);

  const parsed = Number.parseInt(formatted, 10);
  return Number.isNaN(parsed) ? date.getHours() : parsed;
}

function getMinutesInTimezone(date, timeZone) {
  const formatted = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "numeric",
    hour12: false,
    timeZone,
  }).format(date);

  const match = formatted.match(/(\d{1,2}):(\d{2})/);
  if (!match) {
    return date.getHours() * 60 + date.getMinutes();
  }

  const hour = Number.parseInt(match[1], 10);
  const minute = Number.parseInt(match[2], 10);
  if (Number.isNaN(hour) || Number.isNaN(minute)) {
    return date.getHours() * 60 + date.getMinutes();
  }

  return hour * 60 + minute;
}

function isInsideActiveWindow(date = new Date()) {
  if (!isValidHour(ACTIVE_START_HOUR) || !isValidHour(ACTIVE_END_HOUR)) {
    return true;
  }

  if (ACTIVE_START_HOUR === ACTIVE_END_HOUR) {
    return true;
  }

  const hour = getHourInTimezone(date, ACTIVE_TIMEZONE);

  if (ACTIVE_START_HOUR < ACTIVE_END_HOUR) {
    return hour >= ACTIVE_START_HOUR && hour < ACTIVE_END_HOUR;
  }

  return hour >= ACTIVE_START_HOUR || hour < ACTIVE_END_HOUR;
}

function secondsUntilWindowStart(date = new Date()) {
  if (!isValidHour(ACTIVE_START_HOUR) || !isValidHour(ACTIVE_END_HOUR)) {
    return 60;
  }

  if (ACTIVE_START_HOUR === ACTIVE_END_HOUR) {
    return 60;
  }

  const nowMinutes = getMinutesInTimezone(date, ACTIVE_TIMEZONE);
  const startMinutes = ACTIVE_START_HOUR * 60;

  let delta = startMinutes - nowMinutes;
  if (delta <= 0) {
    delta += 24 * 60;
  }

  return Math.max(60, delta * 60);
}

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

async function hasPendingManualLinks() {
  const result = await pool.query(
    `SELECT 1 FROM affiliate_links
     WHERE processed = FALSE
       AND lower(COALESCE(metadata_json->>'manual_priority', 'false')) IN ('true', '1', 'yes', 'on')
     LIMIT 1`
  );
  return result.rows.length > 0;
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
const aiKeywordTemplateCache = new Map();

function normalizeForMatch(value) {
  return cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function matchesKeywordTerm(text, keyword) {
  const normalizedText = normalizeForMatch(text);
  const normalizedKeyword = normalizeForMatch(keyword);

  if (!normalizedText || !normalizedKeyword) {
    return false;
  }

  const keywordPattern = normalizedKeyword
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => escapeRegex(part))
    .join("\\s+");

  if (!keywordPattern) {
    return false;
  }

  const regex = new RegExp(`(^|[^a-z0-9])${keywordPattern}([^a-z0-9]|$)`);
  return regex.test(normalizedText);
}

function findKeywordPosition(text, keyword) {
  const normalizedText = normalizeForMatch(text);
  const normalizedKeyword = normalizeForMatch(keyword);

  if (!normalizedText || !normalizedKeyword) {
    return -1;
  }

  const regex = new RegExp(`(^|[^a-z0-9])(${normalizedKeyword
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => escapeRegex(part))
    .join("\\s+")})(?=[^a-z0-9]|$)`);
  const match = regex.exec(normalizedText);
  if (!match) {
    return -1;
  }

  return match.index + match[1].length;
}

function findBestKeywordMatch(text, keywords) {
  const matches = keywords
    .map((keyword) => {
      const position = findKeywordPosition(text, keyword);
      if (position === -1) {
        return null;
      }

      return {
        keyword,
        position,
        wordCount: normalizeForMatch(keyword).split(/\s+/).filter(Boolean).length,
        length: normalizeForMatch(keyword).length,
      };
    })
    .filter(Boolean)
    .sort((left, right) => {
      if (left.position !== right.position) {
        return left.position - right.position;
      }
      if (left.wordCount !== right.wordCount) {
        return right.wordCount - left.wordCount;
      }
      return right.length - left.length;
    });

  return matches[0] || null;
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
      { headline: "🔥 *Look novo com preço de oportunidade*", hook: "Conforto e estilo para usar hoje." },
      { headline: "✨ *Achadinho feminino do dia*", hook: "Peça versátil que combina com tudo." },
      { headline: "💃 *Seu look merece esse destaque*", hook: "Modelo em alta com ótimo custo-benefício." },
      { headline: "🛍️ *Oferta feminina selecionada para você*", hook: "Confortável para trabalho, passeio e rotina." },
      { headline: "💥 *Calça queridinha em promoção*", hook: "Ideal para montar produções lindas gastando menos." },
    ],
  },
  pijama_babydoll: {
    keywords: ["baby doll", "babydoll", "short doll", "pijama", "pijamas"],
    templates: [
      { headline: "🌙 *Conforto para noites mais leves*", hook: "Pijama lindo e prático para o dia a dia." },
      { headline: "🛌 *Oferta especial de pijama*", hook: "Modelinho fresquinho e confortável para descansar." },
      { headline: "✨ *Seu momento de descanso com estilo*", hook: "Escolha certeira para dormir melhor." },
      { headline: "💖 *Achado de baby doll em destaque*", hook: "Ótimo preço para renovar seu kit de noite." },
      { headline: "🔥 *Pijama feminino com precinho bom*", hook: "Confortável, bonito e pronto para usar." },
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
      { headline: "👔 *Estilo masculino com preço justo*", hook: "Visual alinhado para todas as ocasiões." },
      { headline: "🔥 *Peça masculina em oferta hoje*", hook: "Conforto e qualidade para o dia a dia." },
      { headline: "💼 *Achado masculino selecionado*", hook: "Modelo prático para trabalho e lazer." },
      { headline: "⚡ *Oportunidade para renovar o guarda-roupa*", hook: "Item versátil com excelente custo-benefício." },
      { headline: "🛍️ *Oferta masculina que vale o clique*", hook: "Estilo e conforto no mesmo produto." },
    ],
  },
  audio_fones: {
    keywords: ["fone", "bluetooth", "tws", "earbud", "caixa de som", "som estereo"],
    templates: [
      { headline: "🎧 *Som sem fio com preço de achado*", hook: "Ideal para treino, trabalho e rotina." },
      { headline: "🔊 *Áudio em destaque hoje*", hook: "Modelo queridinho com ótimo custo-benefício." },
      { headline: "⚡ *Upgrade no som sem gastar muito*", hook: "Conexão prática e uso no dia inteiro." },
      { headline: "🔥 *Fone bluetooth em promoção*", hook: "Boa opção para quem curte praticidade." },
      { headline: "🎵 *Achadinho tech para ouvir melhor*", hook: "Produto versátil para todas as horas." },
    ],
  },
  cozinha_utilidades: {
    keywords: [
      "mixer",
      "misturador",
      "batedor",
      "fouet",
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
      { headline: "🥣 *Mixer portátil que facilita a rotina*", hook: "Praticidade para misturar, bater e ganhar tempo na cozinha." },
      { headline: "🍳 *Cozinha mais prática com esse achado*", hook: "Utilidade que facilita sua rotina." },
      { headline: "🏠 *Item de casa em oferta hoje*", hook: "Organização e praticidade em um só produto." },
      { headline: "✨ *Achadinho útil para o dia a dia*", hook: "Perfeito para quem ama praticidade." },
      { headline: "🛒 *Utilidade de cozinha que compensa*", hook: "Produto funcional com preço atraente." },
      { headline: "🔥 *Oferta selecionada para sua cozinha*", hook: "Mais eficiência e conforto na rotina." },
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
      { headline: "🛠️ *Ferramenta útil com preço de oportunidade*", hook: "Boa pedida para reparos e manutenção." },
      { headline: "⚙️ *Achado para quem resolve tudo em casa*", hook: "Prático para obra, ajuste e melhoria." },
      { headline: "🔥 *Produto de reparo em destaque*", hook: "Custo-benefício forte para rotina de casa." },
      { headline: "🏗️ *Item de manutenção que vale ter*", hook: "Mais praticidade para o seu dia a dia." },
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
      { headline: "🛏️ *Conforto para sua casa em oferta*", hook: "Item de enxoval e casa com preço bom." },
      { headline: "🏡 *Achadinho para deixar o lar mais completo*", hook: "Praticidade e conforto na medida certa." },
      { headline: "✨ *Produto de casa em destaque hoje*", hook: "Excelente opção para renovar ambientes." },
      { headline: "🧺 *Oferta de casa e enxoval selecionada*", hook: "Mais conforto para sua rotina." },
      { headline: "🔥 *Item para casa com ótimo custo-benefício*", hook: "Vale conferir enquanto está nesse valor." },
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
      { headline: "💄 *Cuidado pessoal com preço de oportunidade*", hook: "Produto de beleza para sua rotina." },
      { headline: "✨ *Seu autocuidado em destaque hoje*", hook: "Achadinho para se cuidar gastando menos." },
      { headline: "🌸 *Oferta de beleza selecionada*", hook: "Boa opção para manter sua rotina em dia." },
      { headline: "🔥 *Produto de cuidados em promoção*", hook: "Mais praticidade no seu momento de beleza." },
      { headline: "🧴 *Achadinho de beleza que vale o clique*", hook: "Produto útil com excelente custo-benefício." },
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
      { headline: "💪 *Oferta fitness para sua evolução*", hook: "Item funcional para treino e performance." },
      { headline: "🏋️ *Achadinho para rotina de treino*", hook: "Mais resultado com melhor custo-benefício." },
      { headline: "⚡ *Produto esportivo em destaque hoje*", hook: "Boa escolha para quem busca evoluir." },
      { headline: "🔥 *Item fitness com preço de oportunidade*", hook: "Perfeito para complementar seus treinos." },
      { headline: "🚀 *Oferta para quem leva treino a sério*", hook: "Produto prático para sua rotina ativa." },
    ],
  },
  neutro: {
    keywords: [],
    templates: [
      { headline: "🔥 *Achado do dia para economizar*", hook: "Oferta atual com bom custo-benefício." },
      { headline: "✨ *Oferta em destaque agora*", hook: "Item interessante para conferir no link." },
      { headline: "🛒 *Preço bom em produto útil*", hook: "Selecionado para quem gosta de oportunidade." },
      { headline: "⚡ *Oferta com valor competitivo*", hook: "Vale verificar enquanto está disponível." },
      { headline: "📌 *Dica de compra do momento*", hook: "Opção com preço atrativo para hoje." },
    ],
  },
};

const KEYWORD_TEMPLATE_LIBRARY = {
  moda_feminina: {
    "legging": [
      { headline: "🔥 *Legging em destaque hoje*", hook: "Peça curinga para treino, caminhada e rotina com muito conforto." },
      { headline: "✨ *Achado de legging com ótimo caimento*", hook: "Boa opção para quem quer vestir bem sem abrir mão de praticidade." },
      { headline: "💃 *Legging versátil para o dia a dia*", hook: "Combina com looks casuais e também com propostas mais esportivas." },
      { headline: "🛍️ *Oferta de legging para renovar o guarda-roupa*", hook: "Modelo confortável para acompanhar sua rotina do começo ao fim." },
      { headline: "💥 *Legging com preço que vale o clique*", hook: "Uma escolha certeira para quem gosta de conforto e visual atual." },
    ],
    "leg": [
      { headline: "🔥 *Leg em evidência agora*", hook: "Modelo confortável para quem gosta de praticidade no dia a dia." },
      { headline: "✨ *Achadinho de leg para usar muito*", hook: "Boa pedida para montar combinações fáceis e estilosas." },
      { headline: "💃 *Leg com visual moderno e confortável*", hook: "Item versátil para rotina, passeio e momentos de descanso." },
      { headline: "🛍️ *Oferta de leg com ótimo custo-benefício*", hook: "Peça funcional para quem busca conforto sem complicação." },
      { headline: "💥 *Leg selecionada para quem ama conforto*", hook: "Destaque para compor looks leves e fáceis de usar." },
    ],
    "short feminino": [
      { headline: "☀️ *Short feminino com cara de achado*", hook: "Perfeito para dias quentes e combinações leves no dia a dia." },
      { headline: "🛍️ *Oferta de short feminino para aproveitar*", hook: "Peça prática para usar em casa, sair ou montar looks casuais." },
      { headline: "✨ *Short feminino em destaque hoje*", hook: "Boa escolha para quem busca leveza e conforto no vestir." },
      { headline: "💃 *Achadinho de short feminino com bom preço*", hook: "Modelo versátil para deixar a rotina mais confortável e estilosa." },
      { headline: "🔥 *Short feminino que vale conferir*", hook: "Uma opção fácil de combinar e ótima para o calor." },
    ],
    "shorts feminino": [
      { headline: "☀️ *Shorts feminino em promoção*", hook: "Peça leve e prática para os dias mais quentes." },
      { headline: "✨ *Achado de shorts feminino para usar muito*", hook: "Combina com várias propostas e entrega conforto no dia a dia." },
      { headline: "🛍️ *Shorts feminino com preço interessante*", hook: "Boa pedida para quem gosta de roupa confortável e versátil." },
      { headline: "💃 *Shorts feminino em destaque agora*", hook: "Modelo pensado para rotina leve, passeio e momentos casuais." },
      { headline: "🔥 *Oferta boa em shorts feminino*", hook: "Item fácil de combinar e ótimo para manter frescor e estilo." },
    ],
    "pantalona": [
      { headline: "✨ *Pantalona em destaque para hoje*", hook: "Peça elegante e confortável para compor looks mais alinhados." },
      { headline: "🛍️ *Achado de pantalona com ótimo visual*", hook: "Boa escolha para quem gosta de caimento solto e presença no look." },
      { headline: "💃 *Pantalona com estilo e leveza*", hook: "Modelo versátil para trabalho, passeio e ocasiões especiais." },
      { headline: "🔥 *Oferta de pantalona que chama atenção*", hook: "Uma peça marcante para deixar o visual mais sofisticado." },
      { headline: "🌟 *Pantalona com preço que compensa*", hook: "Ideal para quem quer elegância com conforto no mesmo produto." },
    ],
    "saia": [
      { headline: "🌷 *Saia em destaque hoje*", hook: "Peça feminina que deixa o look leve, bonito e fácil de montar." },
      { headline: "🛍️ *Achado de saia para várias ocasiões*", hook: "Boa opção para quem gosta de visual delicado com praticidade." },
      { headline: "✨ *Saia com preço interessante para aproveitar*", hook: "Modelo versátil para usar no dia a dia ou em momentos especiais." },
      { headline: "💃 *Saia que valoriza o visual com leveza*", hook: "Uma escolha certeira para compor looks femininos e confortáveis." },
      { headline: "🔥 *Oferta de saia que vale o clique*", hook: "Peça fácil de combinar e ótima para renovar o guarda-roupa." },
    ],
    "vestido": [
      { headline: "👗 *Vestido em destaque para hoje*", hook: "Peça prática para criar um look bonito sem complicação." },
      { headline: "✨ *Achado de vestido com ótimo visual*", hook: "Boa opção para quem quer conforto e elegância na mesma peça." },
      { headline: "🛍️ *Vestido com preço que vale conferir*", hook: "Modelo versátil para passeio, rotina e momentos especiais." },
      { headline: "💃 *Vestido que já resolve o look*", hook: "Escolha certeira para vestir bem com leveza e praticidade." },
      { headline: "🔥 *Oferta de vestido para renovar o guarda-roupa*", hook: "Uma peça feminina e fácil de usar em várias ocasiões." },
    ],
    "blusa feminina": [
      { headline: "✨ *Blusa feminina em destaque agora*", hook: "Peça versátil para montar combinações bonitas no dia a dia." },
      { headline: "🛍️ *Achado de blusa feminina com bom preço*", hook: "Boa pedida para renovar o guarda-roupa com estilo e leveza." },
      { headline: "🌸 *Blusa feminina para usar muito*", hook: "Modelo fácil de combinar e ótimo para diversas ocasiões." },
      { headline: "🔥 *Oferta de blusa feminina que compensa*", hook: "Uma peça prática para quem gosta de visual bonito e funcional." },
      { headline: "💃 *Blusa feminina com visual atual*", hook: "Escolha interessante para compor looks casuais e elegantes." },
    ],
    "alfaiataria feminina": [
      { headline: "💼 *Alfaiataria feminina em destaque*", hook: "Peça com visual refinado para looks mais elegantes e alinhados." },
      { headline: "✨ *Achado de alfaiataria feminina*", hook: "Boa escolha para quem gosta de sofisticação com versatilidade." },
      { headline: "🛍️ *Oferta de alfaiataria feminina para aproveitar*", hook: "Modelo que eleva o visual e funciona em várias ocasiões." },
      { headline: "🌟 *Alfaiataria feminina com ótimo visual*", hook: "Ideal para compor looks mais arrumados sem perder conforto." },
      { headline: "🔥 *Peça de alfaiataria feminina que vale conferir*", hook: "Uma opção elegante para renovar produções com mais estilo." },
    ],
    "jogger feminina": [
      { headline: "👟 *Jogger feminina em destaque hoje*", hook: "Peça confortável com visual moderno para rotina e passeio." },
      { headline: "✨ *Achadinho de jogger feminina com bom preço*", hook: "Boa opção para quem gosta de conforto sem abrir mão de estilo." },
      { headline: "🛍️ *Jogger feminina para usar muito*", hook: "Modelo versátil para montar looks práticos no dia a dia." },
      { headline: "🔥 *Oferta de jogger feminina que compensa*", hook: "Escolha certeira para quem quer mobilidade e visual atual." },
      { headline: "💃 *Jogger feminina com proposta confortável*", hook: "Peça fácil de combinar e ótima para a rotina." },
    ],
    "calca feminina": [
      { headline: "👖 *Calça feminina em destaque agora*", hook: "Peça essencial para montar looks confortáveis e versáteis." },
      { headline: "✨ *Achado de calça feminina com bom preço*", hook: "Boa pedida para renovar o guarda-roupa com praticidade." },
      { headline: "🛍️ *Oferta de calça feminina para o dia a dia*", hook: "Modelo funcional para rotina, trabalho e passeio." },
      { headline: "🔥 *Calça feminina que vale o clique*", hook: "Escolha interessante para quem busca conforto e estilo." },
      { headline: "💃 *Calça feminina com visual fácil de combinar*", hook: "Uma opção curinga para várias ocasiões." },
    ],
    "calca canelada": [
      { headline: "✨ *Calça canelada em destaque hoje*", hook: "Peça confortável com visual atual para usar bastante." },
      { headline: "🛍️ *Achado de calça canelada com bom caimento*", hook: "Boa escolha para quem gosta de conforto com estilo." },
      { headline: "🔥 *Oferta de calça canelada para aproveitar*", hook: "Modelo versátil para compor looks leves e modernos." },
      { headline: "💃 *Calça canelada com preço interessante*", hook: "Uma opção prática para rotina, descanso e passeio." },
      { headline: "🌟 *Calça canelada que vale conferir*", hook: "Peça fácil de combinar e ótima para renovar produções casuais." },
    ],
    "meia calca": [
      { headline: "🖤 *Meia-calça em destaque agora*", hook: "Item clássico para complementar looks com charme e praticidade." },
      { headline: "✨ *Achado de meia-calça com bom preço*", hook: "Boa opção para compor produções femininas com mais estilo." },
      { headline: "🛍️ *Meia-calça para valorizar o look*", hook: "Detalhe que faz diferença em combinações para várias ocasiões." },
      { headline: "🔥 *Oferta de meia-calça que compensa*", hook: "Escolha interessante para deixar o visual mais elegante." },
      { headline: "🌟 *Meia-calça para completar suas produções*", hook: "Peça versátil para dar acabamento bonito ao look." },
    ],
  },
};

const KEYWORD_TEMPLATE_GUIDES = {
  pijama_babydoll: {
    "baby doll": { label: "Baby Doll", benefit: "leveza e conforto para descansar", useCase: "noites mais tranquilas", appeal: "visual delicado" },
    "babydoll": { label: "Babydoll", benefit: "conforto e toque leve", useCase: "rotina noturna", appeal: "charme no look de dormir" },
    "short doll": { label: "Short Doll", benefit: "frescor e praticidade", useCase: "dias e noites quentes", appeal: "bem-estar no descanso" },
    "pijama": { label: "Pijama", benefit: "conforto para relaxar melhor", useCase: "hora de dormir", appeal: "praticidade no dia a dia" },
    "pijamas": { label: "Pijamas", benefit: "bem-estar e maciez", useCase: "rotina de descanso", appeal: "conforto que faz diferença" },
  },
  moda_masculina: {
    "bermuda": { label: "Bermuda", benefit: "conforto e caimento casual", useCase: "rotina, passeio e fim de semana", appeal: "visual leve" },
    "camisa polo": { label: "Camisa Polo", benefit: "visual alinhado sem esforço", useCase: "trabalho e passeio", appeal: "elegância casual" },
    "camiseta": { label: "Camiseta", benefit: "praticidade e conforto no vestir", useCase: "dia a dia", appeal: "versatilidade" },
    "jogger masculina": { label: "Jogger Masculina", benefit: "mobilidade e conforto", useCase: "rotina e lazer", appeal: "estilo moderno" },
    "calca sarja": { label: "Calça Sarja", benefit: "visual arrumado e fácil de combinar", useCase: "trabalho e saída", appeal: "acabamento elegante" },
    "jaqueta masculina": { label: "Jaqueta Masculina", benefit: "proteção com estilo", useCase: "dias amenos e noites", appeal: "presença no look" },
    "shorts masculino": { label: "Shorts Masculino", benefit: "leveza e conforto", useCase: "dias quentes", appeal: "visual descontraído" },
    "calca masculina": { label: "Calça Masculina", benefit: "conforto para usar bastante", useCase: "rotina e trabalho", appeal: "peça curinga" },
  },
  audio_fones: {
    "fone": { label: "Fone", benefit: "áudio prático no dia a dia", useCase: "trabalho, treino e lazer", appeal: "comodidade" },
    "bluetooth": { label: "Bluetooth", benefit: "liberdade sem fio", useCase: "rotina corrida", appeal: "praticidade" },
    "tws": { label: "TWS", benefit: "uso sem fio com mais mobilidade", useCase: "música e chamadas", appeal: "modernidade" },
    "earbud": { label: "Earbud", benefit: "conforto no uso prolongado", useCase: "dia a dia", appeal: "discrição" },
    "caixa de som": { label: "Caixa de Som", benefit: "som para curtir melhor os momentos", useCase: "casa e encontros", appeal: "potência e diversão" },
    "som estereo": { label: "Som Estéreo", benefit: "experiência sonora mais envolvente", useCase: "música e vídeos", appeal: "qualidade de áudio" },
  },
  cozinha_utilidades: {
    "marmita": { label: "Marmita", benefit: "praticidade para organizar refeições", useCase: "trabalho e rotina", appeal: "economia e organização" },
    "mixer": { label: "Mixer", benefit: "mais agilidade no preparo", useCase: "bebidas, ovos e receitas rápidas", appeal: "praticidade na cozinha" },
    "misturador": { label: "Misturador", benefit: "mistura rápida e sem esforço", useCase: "preparo do dia a dia", appeal: "agilidade" },
    "batedor": { label: "Batedor", benefit: "mais facilidade no preparo", useCase: "receitas e bebidas", appeal: "praticidade" },
    "fouet": { label: "Fouet", benefit: "mistura mais prática", useCase: "receitas rápidas", appeal: "utilidade" },
    "pote": { label: "Pote", benefit: "organização e conservação", useCase: "cozinha do dia a dia", appeal: "praticidade" },
    "fatiador": { label: "Fatiador", benefit: "mais agilidade no preparo", useCase: "cozinha diária", appeal: "facilidade" },
    "cozinha": { label: "Item de Cozinha", benefit: "rotina mais funcional", useCase: "tarefas do dia a dia", appeal: "utilidade" },
    "panela": { label: "Panela", benefit: "mais praticidade no preparo", useCase: "refeições do dia", appeal: "funcionalidade" },
    "chaleira": { label: "Chaleira", benefit: "agilidade para bebidas e preparo", useCase: "café e chá", appeal: "conveniência" },
    "escorredor": { label: "Escorredor", benefit: "organização e praticidade", useCase: "pia e cozinha", appeal: "rotina mais leve" },
    "porta temperos": { label: "Porta Temperos", benefit: "mais ordem e acesso fácil", useCase: "cozinha organizada", appeal: "praticidade" },
    "faqueiro": { label: "Faqueiro", benefit: "mesa mais completa", useCase: "refeições e ocasiões especiais", appeal: "acabamento bonito" },
    "tacas": { label: "Taças", benefit: "mesa mais bonita e convidativa", useCase: "momentos especiais", appeal: "elegância" },
    "xicara": { label: "Xícara", benefit: "mais charme para o café", useCase: "rotina e visitas", appeal: "delicadeza" },
    "coador": { label: "Coador", benefit: "preparo simples e funcional", useCase: "café do dia a dia", appeal: "praticidade" },
    "torneira": { label: "Torneira", benefit: "mais praticidade e organização", useCase: "cozinha ou banheiro", appeal: "funcionalidade" },
    "utensilios": { label: "Utensílios", benefit: "mais eficiência na cozinha", useCase: "preparo diário", appeal: "utilidade" },
  },
  ferramentas_reparo: {
    "broca": { label: "Broca", benefit: "precisão e praticidade no uso", useCase: "reparos e instalações", appeal: "resultado melhor" },
    "parafusadeira": { label: "Parafusadeira", benefit: "mais agilidade no serviço", useCase: "montagem e reparo", appeal: "praticidade" },
    "catraca": { label: "Catraca", benefit: "facilidade no ajuste", useCase: "manutenção do dia a dia", appeal: "uso funcional" },
    "ferramenta": { label: "Ferramenta", benefit: "apoio útil para reparos", useCase: "casa e rotina", appeal: "resolver sem complicação" },
    "pistola": { label: "Pistola", benefit: "aplicação mais prática", useCase: "acabamento e reparo", appeal: "agilidade" },
    "veda": { label: "Veda", benefit: "melhor proteção e vedação", useCase: "reparos rápidos", appeal: "eficiência" },
    "impermeabil": { label: "Impermeabilizante", benefit: "mais proteção contra umidade", useCase: "casa e manutenção", appeal: "durabilidade" },
    "manta liquida": { label: "Manta Líquida", benefit: "vedação prática", useCase: "áreas que precisam de proteção", appeal: "segurança" },
    "tinta": { label: "Tinta", benefit: "renovação com bom acabamento", useCase: "pintura e retoque", appeal: "visual renovado" },
    "verniz": { label: "Verniz", benefit: "proteção e acabamento", useCase: "madeira e superfícies", appeal: "durabilidade" },
    "spray": { label: "Spray", benefit: "aplicação rápida e simples", useCase: "retoques e manutenção", appeal: "praticidade" },
    "chave": { label: "Chave", benefit: "uso útil no dia a dia", useCase: "ajustes e reparos", appeal: "item necessário" },
    "alicate": { label: "Alicate", benefit: "apoio firme para reparos", useCase: "manutenção geral", appeal: "funcionalidade" },
    "pulverizador": { label: "Pulverizador", benefit: "aplicação uniforme", useCase: "jardim e limpeza", appeal: "facilidade" },
  },
  casa_enxoval: {
    "lencol": { label: "Lençol", benefit: "mais conforto na hora de descansar", useCase: "quarto e rotina", appeal: "bem-estar" },
    "lençol": { label: "Lençol", benefit: "mais conforto na hora de descansar", useCase: "quarto e rotina", appeal: "bem-estar" },
    "coberdrom": { label: "Coberdrom", benefit: "aconchego para noites mais tranquilas", useCase: "dias frios", appeal: "conforto" },
    "cobertor": { label: "Cobertor", benefit: "mais calor e aconchego", useCase: "descanso", appeal: "maciez" },
    "travesseiro": { label: "Travesseiro", benefit: "apoio melhor no descanso", useCase: "hora de dormir", appeal: "comodidade" },
    "toalha": { label: "Toalha", benefit: "rotina mais confortável", useCase: "banho e dia a dia", appeal: "maciez" },
    "capa protetora colchao": { label: "Capa Protetora de Colchão", benefit: "mais proteção e cuidado", useCase: "quarto organizado", appeal: "durabilidade" },
    "capa de colchao": { label: "Capa de Colchão", benefit: "proteção prática para o colchão", useCase: "dia a dia", appeal: "cuidado" },
    "enxoval": { label: "Enxoval", benefit: "mais conforto para a casa", useCase: "quarto e rotina", appeal: "ambiente completo" },
    "mesa de cabeceira": { label: "Mesa de Cabeceira", benefit: "organização e apoio ao lado da cama", useCase: "quarto", appeal: "praticidade" },
    "quadro decorativo": { label: "Quadro Decorativo", benefit: "visual mais bonito no ambiente", useCase: "decoração", appeal: "estilo" },
    "garrafa termica": { label: "Garrafa Térmica", benefit: "bebida na temperatura certa por mais tempo", useCase: "casa e trabalho", appeal: "conveniência" },
  },
  beleza_cuidados: {
    "body splash": { label: "Body Splash", benefit: "fragrância leve e agradável", useCase: "dia a dia", appeal: "sensação de frescor" },
    "mascara": { label: "Máscara", benefit: "cuidado extra na rotina", useCase: "autocuidado", appeal: "resultado visível" },
    "cabelo": { label: "Cuidado para Cabelo", benefit: "mais atenção aos fios", useCase: "rotina de beleza", appeal: "aparência mais bonita" },
    "anti idade": { label: "Anti-idade", benefit: "cuidado com a pele", useCase: "rotina diária", appeal: "autocuidado" },
    "anti-idade": { label: "Anti-idade", benefit: "cuidado com a pele", useCase: "rotina diária", appeal: "autocuidado" },
    "massageador": { label: "Massageador", benefit: "alívio e bem-estar", useCase: "momentos de descanso", appeal: "relaxamento" },
    "perfume": { label: "Perfume", benefit: "presença marcante e fragrância agradável", useCase: "uso diário e ocasiões especiais", appeal: "identidade" },
    "attraction": { label: "Attraction", benefit: "fragrância de presença", useCase: "dia a dia e ocasiões especiais", appeal: "sofisticação" },
    "hidratante": { label: "Hidratante", benefit: "pele mais macia e cuidada", useCase: "rotina diária", appeal: "autocuidado" },
    "botox": { label: "Botox Capilar", benefit: "cuidado e alinhamento dos fios", useCase: "rotina de cabelo", appeal: "beleza" },
    "hyaluron": { label: "Hyaluron", benefit: "cuidado facial com toque leve", useCase: "skincare", appeal: "tratamento" },
    "beleza": { label: "Item de Beleza", benefit: "mais cuidado na rotina", useCase: "autocuidado diário", appeal: "bem-estar" },
    "seringa": { label: "Seringa de Aplicação", benefit: "uso prático e funcional", useCase: "cuidados específicos", appeal: "precisão" },
  },
  fitness_saude: {
    "creatina": { label: "Creatina", benefit: "apoio para performance na rotina de treino", useCase: "academia e esporte", appeal: "resultado" },
    "fitness": { label: "Item Fitness", benefit: "mais apoio para sua rotina ativa", useCase: "treino", appeal: "evolução" },
    "academia": { label: "Item para Academia", benefit: "mais praticidade no treino", useCase: "rotina esportiva", appeal: "desempenho" },
    "bioimpedancia": { label: "Bioimpedância", benefit: "acompanhamento mais claro da rotina", useCase: "saúde e treino", appeal: "controle" },
    "mini band": { label: "Mini Band", benefit: "treinos variados com praticidade", useCase: "exercícios e mobilidade", appeal: "versatilidade" },
    "treino": { label: "Item de Treino", benefit: "mais apoio para evoluir", useCase: "rotina fitness", appeal: "constância" },
    "esportivo": { label: "Item Esportivo", benefit: "mais funcionalidade para rotina ativa", useCase: "atividade física", appeal: "performance" },
    "caminhada": { label: "Item para Caminhada", benefit: "mais conforto no movimento", useCase: "atividade leve e diária", appeal: "bem-estar" },
    "fisioterapia": { label: "Item para Fisioterapia", benefit: "apoio em exercícios e recuperação", useCase: "cuidados físicos", appeal: "funcionalidade" },
    "suplement": { label: "Suplemento", benefit: "apoio para rotina de resultado", useCase: "treino e alimentação", appeal: "performance" },
  },
};

const NEUTRAL_TOPIC_RULES = [
  {
    topic: "moda",
    keywords: ["short", "shorts", "calca", "camisa", "camiseta", "blusa", "saia", "vestido", "legging", "bermuda", "polo", "jogger"],
    templates: [
      { headline: "🛍️ *Achado de moda com preço baixo*", hook: "Peça em destaque para renovar o guarda-roupa." },
      { headline: "✨ *Oferta de moda para aproveitar*", hook: "Item versátil com valor interessante hoje." },
      { headline: "🔥 *Moda em promoção no momento*", hook: "Boa oportunidade para comprar pagando menos." },
    ],
  },
  {
    topic: "beleza",
    keywords: ["perfume", "body splash", "hidratante", "cabelo", "mascara", "massageador", "botox", "hyaluron", "serum"],
    templates: [
      { headline: "💄 *Oferta de beleza em destaque*", hook: "Produto de cuidados com preço atrativo." },
      { headline: "🌸 *Achadinho de autocuidado hoje*", hook: "Item de beleza com custo-benefício interessante." },
      { headline: "✨ *Preço bom para cuidar de você*", hook: "Vale conferir essa oportunidade de beleza." },
    ],
  },
  {
    topic: "casa",
    keywords: ["cozinha", "panela", "marmita", "pote", "toalha", "lencol", "travesseiro", "utensilio", "escorredor", "torneira"],
    templates: [
      { headline: "🏠 *Utilidade para casa com preço bom*", hook: "Produto prático para facilitar a rotina." },
      { headline: "🧺 *Oferta para o dia a dia da casa*", hook: "Item funcional com valor competitivo." },
      { headline: "🍳 *Achado útil para sua rotina*", hook: "Boa opção para organizar e simplificar tarefas." },
    ],
  },
  {
    topic: "tecnologia",
    keywords: ["fone", "bluetooth", "tws", "caixa de som", "earbud", "carregador", "smart", "gadget"],
    templates: [
      { headline: "🎧 *Oferta de tecnologia em destaque*", hook: "Produto prático com preço interessante hoje." },
      { headline: "⚡ *Achado tech com bom valor*", hook: "Boa oportunidade para quem curte tecnologia." },
      { headline: "🔊 *Item de áudio e tech para conferir*", hook: "Custo-benefício atrativo no momento." },
    ],
  },
  {
    topic: "saude_fitness",
    keywords: ["creatina", "fitness", "treino", "academia", "suplement", "fisioterapia", "bioimpedancia"],
    templates: [
      { headline: "💪 *Oferta fitness com preço competitivo*", hook: "Item em destaque para sua rotina de treino." },
      { headline: "🏋️ *Achado esportivo do momento*", hook: "Produto útil para quem busca performance." },
      { headline: "🚀 *Preço bom para evoluir no treino*", hook: "Vale conferir essa opção fitness." },
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
  let bestGroupMatch = null;

  for (const [groupKey, groupConfig] of Object.entries(MESSAGE_TEMPLATE_GROUPS)) {
    if (groupKey === "neutro") {
      continue;
    }

    const matchedKeyword = findBestKeywordMatch(productName, groupConfig.keywords);
    if (!matchedKeyword) {
      continue;
    }

    if (
      !bestGroupMatch ||
      matchedKeyword.position < bestGroupMatch.position ||
      (matchedKeyword.position === bestGroupMatch.position && matchedKeyword.wordCount > bestGroupMatch.wordCount) ||
      (matchedKeyword.position === bestGroupMatch.position && matchedKeyword.wordCount === bestGroupMatch.wordCount && matchedKeyword.length > bestGroupMatch.length)
    ) {
      bestGroupMatch = { groupKey, ...matchedKeyword };
    }
  }

  if (bestGroupMatch) {
    return bestGroupMatch.groupKey;
  }

  const categoryGroup = resolveCategoryHintGroup(metadata);
  if (categoryGroup) {
    return categoryGroup;
  }

  return "neutro";
}

function resolveMatchedKeyword(productName) {
  let bestMatch = null;

  for (const [groupKey, groupConfig] of Object.entries(MESSAGE_TEMPLATE_GROUPS)) {
    if (groupKey === "neutro") {
      continue;
    }

    const matchedKeyword = findBestKeywordMatch(productName, groupConfig.keywords);
    if (!matchedKeyword) {
      continue;
    }

    if (
      !bestMatch ||
      matchedKeyword.position < bestMatch.position ||
      (matchedKeyword.position === bestMatch.position && matchedKeyword.wordCount > bestMatch.wordCount) ||
      (matchedKeyword.position === bestMatch.position && matchedKeyword.wordCount === bestMatch.wordCount && matchedKeyword.length > bestMatch.length)
    ) {
      bestMatch = matchedKeyword;
    }
  }

  return bestMatch ? bestMatch.keyword : "";
}

function normalizeKeywordKey(keyword) {
  return normalizeForMatch(keyword).replace(/\s+/g, "_");
}

function titleCaseKeyword(keyword) {
  const clean = cleanText(keyword);
  if (!clean) {
    return "produto";
  }

  return clean
    .split(/\s+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function buildKeywordTemplates(keyword, groupKey) {
  const explicitTemplates = KEYWORD_TEMPLATE_LIBRARY[groupKey]?.[keyword];
  if (Array.isArray(explicitTemplates) && explicitTemplates.length >= 3) {
    return explicitTemplates;
  }

  const guide = KEYWORD_TEMPLATE_GUIDES[groupKey]?.[keyword];
  if (guide) {
    const iconMap = {
      pijama_babydoll: "🌙",
      moda_masculina: "👔",
      audio_fones: "🎧",
      cozinha_utilidades: "🍳",
      ferramentas_reparo: "🛠️",
      casa_enxoval: "🏠",
      beleza_cuidados: "💄",
      fitness_saude: "💪",
    };

    const icon = iconMap[groupKey] || "🔥";
    return [
      { headline: `${icon} *${guide.label} em destaque hoje*`, hook: `${guide.benefit.charAt(0).toUpperCase() + guide.benefit.slice(1)} para ${guide.useCase}.` },
      { headline: `${icon} *Achado de ${guide.label} para aproveitar*`, hook: `Boa escolha para quem busca ${guide.appeal} com mais praticidade.` },
      { headline: `${icon} *${guide.label} com preço interessante*`, hook: `Uma opção pensada para ${guide.useCase} com ${guide.benefit}.` },
      { headline: `${icon} *Oferta de ${guide.label} que vale o clique*`, hook: `Produto com foco em ${guide.appeal} e uso útil no dia a dia.` },
      { headline: `${icon} *${guide.label}: oportunidade do momento*`, hook: `Destaque para quem quer ${guide.benefit} sem complicação.` },
    ];
  }

  const label = titleCaseKeyword(keyword);
  const groupPrefixMap = {
    moda_feminina: "👗",
    pijama_babydoll: "🌙",
    moda_masculina: "👔",
    audio_fones: "🎧",
    cozinha_utilidades: "🍳",
    ferramentas_reparo: "🛠️",
    casa_enxoval: "🏠",
    beleza_cuidados: "💄",
    fitness_saude: "💪",
  };

  const icon = groupPrefixMap[groupKey] || "🔥";

  const keywordFocusHints = {
    "bermuda": "caimento confortável e uso diário",
    "camisa polo": "visual alinhado e versatilidade",
    "camiseta": "conforto e praticidade",
    "legging": "mobilidade e conforto",
    "short feminino": "leveza para dias quentes",
    "perfume": "fixação e presença marcante",
    "body splash": "fragrância leve para o dia",
    "massageador": "alívio e bem-estar",
    "creatina": "desempenho e rotina de treino",
    "fone": "áudio limpo e praticidade",
    "bluetooth": "conexão sem fio no dia a dia",
    "cozinha": "agilidade e organização",
    "ferramenta": "uso prático para reparos",
  };

  const focus = keywordFocusHints[normalizeForMatch(keyword)] || `benefício real para quem procura ${keyword}`;

  return [
    {
      headline: `${icon} *${label} em destaque hoje*`,
      hook: `Seleção focada em ${keyword}, com ${focus}.`,
    },
    {
      headline: `${icon} *${label} com preço competitivo*`,
      hook: `Boa oportunidade para aproveitar ${keyword} com custo-benefício interessante.`,
    },
    {
      headline: `${icon} *Achado de ${label} para hoje*`,
      hook: `Oferta pensada para quem quer ${keyword} sem pagar caro.`,
    },
    {
      headline: `${icon} *${label}: oferta selecionada*`,
      hook: `Destaque de ${keyword} com proposta clara: ${focus}.`,
    },
  ];
}

function parseJsonFromText(text) {
  const raw = cleanText(text);
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    const start = raw.indexOf("{");
    const end = raw.lastIndexOf("}");
    if (start === -1 || end === -1 || end <= start) {
      return null;
    }

    try {
      return JSON.parse(raw.slice(start, end + 1));
    } catch (innerError) {
      return null;
    }
  }
}

function normalizeTemplateCandidate(candidate) {
  if (!candidate || typeof candidate !== "object") {
    return null;
  }

  const headline = cleanText(candidate.headline);
  const hook = cleanText(candidate.hook);
  if (!headline || !hook) {
    return null;
  }

  return { headline, hook, templateSource: "ai" };
}

async function generateKeywordTemplatesWithAI(keyword, groupKey) {
  if (!AI_COPY_ENABLED || !AI_COPY_API_KEY || !AI_COPY_API_URL || !AI_COPY_MODEL) {
    return null;
  }

  const prompt = [
    "Gere copy de WhatsApp para vendas em PT-BR.",
    `Keyword: ${keyword}`,
    `Grupo: ${groupKey}`,
    "Retorne SOMENTE JSON valido no formato:",
    '{"templates":[{"headline":"...","hook":"..."},{"headline":"...","hook":"..."},{"headline":"...","hook":"..."}] }',
    "Regras:",
    "- Exatamente 3 templates.",
    "- Headline: texto curto com 1 emoji no inicio e a parte mais impactante entre asteriscos para negrito no WhatsApp. Exemplo: '🎧 *Audio sem fio com preco de achado*'",
    "- Hook: 1 frase destacando o beneficio real. Voce pode colocar 1 palavra ou expressao chave entre asteriscos para negrito. Exemplo: 'Experiente a *liberdade* do audio sem fio.'",
    "- Use portugues do Brasil com acentuacao correta e ortografia impecavel.",
    "- Escreva no tom de um vendedor de alta performance, focado em converter.",
    "- Nao use titulos genericos como 'Produto que surpreende' ou 'Novidade incrivel'.",
    "- Nao inventar desconto, urgencia falsa, nem promessas irreais.",
    "- Texto natural para grupo grande de WhatsApp.",
  ].join("\n");

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), AI_COPY_TIMEOUT_MS);

    const response = await fetch(AI_COPY_API_URL, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${AI_COPY_API_KEY}`,
      },
      body: JSON.stringify({
        model: AI_COPY_MODEL,
        temperature: 0.9,
        max_tokens: 350,
        messages: [
          {
            role: "system",
            content:
              "Voce escreve copy comercial em portugues do Brasil para WhatsApp. Use acentuacao correta, linguagem natural e tom persuasivo de vendedor top. Use *asteriscos* para negrito no WhatsApp: coloque o trecho mais impactante do headline entre asteriscos, e destaque 1 palavra chave no hook. Nunca use titulos genericos ou promessas falsas.",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
      }),
    });

    clearTimeout(timeout);

    if (!response.ok) {
      logger.warn({ status: response.status, keyword }, "IA de copy retornou erro HTTP.");
      return null;
    }

    const data = await response.json();
    const content = cleanText(data?.choices?.[0]?.message?.content);
    const parsed = parseJsonFromText(content);
    const rawTemplates = Array.isArray(parsed?.templates) ? parsed.templates : [];
    const templates = rawTemplates
      .map(normalizeTemplateCandidate)
      .filter(Boolean)
      .slice(0, 3);

    if (templates.length < 3) {
      logger.warn({ keyword }, "IA de copy nao retornou 3 templates validos. Usando fallback local.");
      return null;
    }

    return templates;
  } catch (error) {
    logger.warn({ err: error, keyword }, "Falha ao gerar templates via IA. Usando fallback local.");
    return null;
  }
}

async function getKeywordTemplates(keyword, groupKey, options = {}) {
  const { cachePrefix = "keyword", fallbackFactory = null } = options;
  const keywordKey = `${cachePrefix}:${normalizeKeywordKey(keyword)}`;

  if (aiKeywordTemplateCache.has(keywordKey)) {
    return aiKeywordTemplateCache.get(keywordKey);
  }

  const aiTemplates = await generateKeywordTemplatesWithAI(keyword, groupKey);
  const fallbackTemplates = fallbackFactory ? fallbackFactory() : buildKeywordTemplates(keyword, groupKey);
  const templates = (aiTemplates || fallbackTemplates).map((template) => ({
    ...template,
    templateSource: template.templateSource || (aiTemplates ? "ai" : "local"),
  }));

  aiKeywordTemplateCache.set(keywordKey, templates);
  return templates;
}

async function pickTemplateForKeyword(keyword, groupKey) {
  const templates = await getKeywordTemplates(keyword, groupKey);
  const keywordKey = normalizeKeywordKey(keyword);
  const memoryKey = `keyword_${keywordKey}`;

  const previousIndex = lastTemplateIndexByGroup.get(memoryKey);
  const candidateIndexes = templates
    .map((_, index) => index)
    .filter((index) => index !== previousIndex);

  const randomPool = candidateIndexes.length > 0 ? candidateIndexes : [0];
  const selectedIndex = randomPool[Math.floor(Math.random() * randomPool.length)];
  lastTemplateIndexByGroup.set(memoryKey, selectedIndex);

  return templates[selectedIndex];
}

async function pickTemplateForGroup(groupKey, productName) {
  const groupConfig = MESSAGE_TEMPLATE_GROUPS[groupKey] || MESSAGE_TEMPLATE_GROUPS.neutro;
  const templates = await getKeywordTemplates(productName, groupKey, {
    cachePrefix: `group_${groupKey}`,
    fallbackFactory: () => groupConfig.templates,
  });

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

  return {
    ...templates[selectedIndex],
    templateSource: templates[selectedIndex].templateSource || "local",
  };
}

function resolveNeutralTopicConfig(productName) {
  const normalizedName = normalizeForMatch(productName);

  for (const rule of NEUTRAL_TOPIC_RULES) {
    const matched = rule.keywords.some((keyword) => matchesKeywordTerm(normalizedName, keyword));
    if (matched) {
      return rule;
    }
  }

  return null;
}

async function pickTemplateForNeutralTopic(productName) {
  const topicConfig = resolveNeutralTopicConfig(productName);
  const topicKey = topicConfig?.topic || "geral";
  const templates = await getKeywordTemplates(productName, topicKey, {
    cachePrefix: `neutral_${topicKey}`,
    fallbackFactory: () => topicConfig?.templates || MESSAGE_TEMPLATE_GROUPS.neutro.templates,
  });
  const memoryKey = `neutro_${topicKey}`;

  const previousIndex = lastTemplateIndexByGroup.get(memoryKey);
  const candidateIndexes = templates
    .map((_, index) => index)
    .filter((index) => index !== previousIndex);

  const randomPool = candidateIndexes.length > 0 ? candidateIndexes : [0];
  const selectedIndex = randomPool[Math.floor(Math.random() * randomPool.length)];
  lastTemplateIndexByGroup.set(memoryKey, selectedIndex);

  return {
    ...templates[selectedIndex],
    templateSource: templates[selectedIndex].templateSource || "local",
  };
}

async function buildMessage(link, enrichment) {
  const productName = cleanText(enrichment.productName || link.product_name || "Produto sem nome");
  const metadata = {
    ...normalizeMetadata(link.metadata_json),
    ...normalizeMetadata(enrichment.metadata),
  };

  const salesValue = pickMeta(metadata.sales, metadata.sold, metadata.historical_sold);
  const shopValue = pickMeta(metadata.shop_name, metadata.shopName, metadata.store_name);
  const pricing = resolveOfferPricing(link, enrichment, metadata);

  const hasBothPrices = !!(pricing.originalPriceText && enrichment.priceText);
  const originalPriceLabel = hasBothPrices ? `💸 De: ~${pricing.originalPriceText}~` : "";
  const priceLabel = enrichment.priceText
    ? hasBothPrices
      ? `💰 *Por:* ${enrichment.priceText}`
      : `💰 *Preço:* ${enrichment.priceText}`
    : "";
  const discountLabel = pricing.discountText ? `🎯 *Desconto:* ${pricing.discountText}` : "";
  const salesCount = parseSalesCount(salesValue);
  const salesLabel = salesValue && salesCount !== null && salesCount >= 300 ? `🔥 *Vendas:* ${salesValue}` : "";
  const shopLabel = shopValue ? `🏬 *Loja:* ${shopValue}` : "";
  const productLabel = `📦 *Produto:* ${productName}`;
  const linkLabel = `🛒 *Compre aqui:* ${link.affiliate_url}`;

  const groupKey = resolveProductGroupKey(productName, metadata);
  const matchedKeyword = resolveMatchedKeyword(productName);

  let selectedTemplate;
  if (matchedKeyword) {
    selectedTemplate = await pickTemplateForKeyword(matchedKeyword, groupKey);
  } else if (groupKey === "neutro") {
    selectedTemplate = await pickTemplateForNeutralTopic(productName);
  } else {
    selectedTemplate = await pickTemplateForGroup(groupKey, productName);
  }
  const hookLine = cleanText(selectedTemplate.hook);

  const priceBlock = [originalPriceLabel, priceLabel, discountLabel].filter(Boolean).join("\n");
  const detailsBlock = [salesLabel, shopLabel].filter(Boolean).join("\n");

  const text = [
    selectedTemplate.headline.toUpperCase(),
    hookLine,
    "",
    productLabel,
    priceBlock,
    detailsBlock,
    linkLabel,
  ]
    .filter(Boolean)
    .join("\n\n");

  return {
    text,
    templateSource: selectedTemplate.templateSource || "local",
    templateHeadline: cleanText(selectedTemplate.headline),
  };
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

      if (!isInsideActiveWindow()) {
        if (await hasPendingManualLinks()) {
          logger.info(
            { activeStartHour: ACTIVE_START_HOUR, activeEndHour: ACTIVE_END_HOUR },
            "Fora da janela ativa, mas ha item com manual_priority pendente. Prosseguindo."
          );
        } else {
          const waitSeconds = secondsUntilWindowStart();
          logger.info(
            {
              activeStartHour: isValidHour(ACTIVE_START_HOUR) ? ACTIVE_START_HOUR : null,
              activeEndHour: isValidHour(ACTIVE_END_HOUR) ? ACTIVE_END_HOUR : null,
              activeTimezone: ACTIVE_TIMEZONE,
              waitSeconds,
            },
            "Fora da janela ativa de envio. Aguardando proxima janela."
          );
          await delay(waitSeconds * 1000);
          continue;
        }
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

        const messageResult = await buildMessage(link, enrichment);
        const message = messageResult.text;

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
            templateSource: messageResult.templateSource,
            templateHeadline: messageResult.templateHeadline,
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
