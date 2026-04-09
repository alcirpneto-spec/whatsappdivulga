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
const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 15);
const MAX_LINKS_PER_CYCLE = Number(process.env.MAX_LINKS_PER_CYCLE || 10);
const BAILEYS_GROUP_JID = process.env.BAILEYS_GROUP_JID;
const WHATSAPP_GROUP_NAME = process.env.WHATSAPP_GROUP_NAME;

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL nao definido.");
}

const pool = new Pool({ connectionString: DATABASE_URL });

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function prettySource(source) {
  const value = String(source || "mercado_livre").replace(/_/g, " ").trim();
  if (!value) {
    return "Mercado Livre";
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

async function fetchEnrichment(link) {
  const fromDbPrice = cleanText(link.price_text);
  const fromDbImage = cleanText(link.image_url);

  if (fromDbPrice && fromDbImage) {
    return {
      priceText: formatPrice(fromDbPrice),
      imageUrl: fromDbImage,
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
      };
    }

    const html = await response.text();

    const metaPrice =
      extractMetaContent(html, "product:price:amount") ||
      extractMetaContent(html, "og:price:amount") ||
      extractMetaContent(html, "twitter:data1");

    const regexPrice =
      extractRegexValue(html, /"price"\s*:\s*"([0-9.,]+)"/i) ||
      extractRegexValue(html, /R\$\s*([0-9.]+,[0-9]{2})/i);

    const metaImage =
      extractMetaContent(html, "og:image") ||
      extractMetaContent(html, "twitter:image") ||
      extractMetaContent(html, "twitter:image:src");

    return {
      priceText: formatPrice(fromDbPrice || metaPrice || regexPrice),
      imageUrl: cleanText(fromDbImage || metaImage),
    };
  } catch (error) {
    logger.warn({ err: error, linkId: link.id }, "Falha ao enriquecer dados do link. Vou enviar sem imagem/preco se necessario.");
    return {
      priceText: formatPrice(fromDbPrice),
      imageUrl: fromDbImage,
    };
  }
}

async function persistEnrichment(id, enrichment) {
  await pool.query(
    `
      UPDATE affiliate_links
      SET price_text = COALESCE(NULLIF($2, ''), price_text),
          image_url = COALESCE(NULLIF($3, ''), image_url)
      WHERE id = $1
    `,
    [id, enrichment.priceText || "", enrichment.imageUrl || ""]
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

    CREATE INDEX IF NOT EXISTS idx_affiliate_links_pending
      ON affiliate_links (processed, created_at);
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
      FROM affiliate_links
      WHERE processed = FALSE
      ORDER BY created_at ASC
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

function buildMessage(link, enrichment) {
  const sourceLabel = `Fonte: ${prettySource(link.source)}`;
  const priceLabel = enrichment.priceText ? `Preco: ${enrichment.priceText}` : "";

  return [
    "Oferta feita pra voce!",
    "",
    `Produto: ${link.product_name}`,
    priceLabel,
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

    while (connection.shouldReconnect()) {
      if (!connection.isReady()) {
        await delay(2000);
        continue;
      }

      let groupJid;
      try {
        groupJid = await resolveGroupJid(connection.sock);
      } catch (error) {
        logger.error({ err: error }, "Falha ao resolver grupo. Vou tentar novamente.");
        await delay(POLL_INTERVAL_SECONDS * 1000);
        continue;
      }

      const pendingLinks = await getPendingLinks(MAX_LINKS_PER_CYCLE);

      if (pendingLinks.length === 0) {
        await delay(POLL_INTERVAL_SECONDS * 1000);
        continue;
      }

      logger.info(`Encontrados ${pendingLinks.length} links pendentes.`);

      for (const link of pendingLinks) {
        try {
          const enrichment = await fetchEnrichment(link);
          await persistEnrichment(link.id, enrichment);

          const message = buildMessage(link, enrichment);

          if (enrichment.imageUrl) {
            try {
              await connection.sock.sendMessage(groupJid, {
                image: { url: enrichment.imageUrl },
                caption: message,
              });
            } catch (imageError) {
              logger.warn({ err: imageError, linkId: link.id }, "Falha ao enviar imagem. Vou enviar texto.");
              await connection.sock.sendMessage(groupJid, { text: message });
            }
          } else {
            await connection.sock.sendMessage(groupJid, { text: message });
          }

          await markAsSent(link.id);
          logger.info(`Link ${link.id} enviado com sucesso.`);
        } catch (error) {
          const errorMessage = error && error.message ? error.message.slice(0, 500) : "Erro desconhecido";
          await markAsFailed(link.id, errorMessage);
          logger.error({ err: error, linkId: link.id }, "Falha ao enviar link.");
        }
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
