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

async function ensureSchema() {
  const query = `
    CREATE TABLE IF NOT EXISTS affiliate_links (
      id BIGSERIAL PRIMARY KEY,
      product_name TEXT NOT NULL,
      affiliate_url TEXT NOT NULL,
      source TEXT DEFAULT 'mercado_livre',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      processed BOOLEAN NOT NULL DEFAULT FALSE,
      sent_at TIMESTAMPTZ,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT
    );

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
      SELECT id, product_name, affiliate_url, source, created_at, attempts
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

function buildMessage(link) {
  const sourceLabel = link.source ? `Fonte: ${link.source}` : "Fonte: mercado_livre";

  return [
    "Oferta nova de afiliado",
    "",
    `Produto: ${link.product_name}`,
    sourceLabel,
    `Link: ${link.affiliate_url}`,
  ].join("\n");
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
        logger.error("Sessao desconectada (loggedOut). Remova ./auth e autentique novamente.");
        return;
      }

      logger.warn({ statusCode }, "Conexao fechada. Tentando reconectar...");
    }
  });

  return {
    sock,
    isReady: () => isReady,
    shouldReconnect: () => shouldReconnect,
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
          const message = buildMessage(link);
          await connection.sock.sendMessage(groupJid, { text: message });
          await markAsSent(link.id);
          logger.info(`Link ${link.id} enviado com sucesso.`);
        } catch (error) {
          const errorMessage = error && error.message ? error.message.slice(0, 500) : "Erro desconhecido";
          await markAsFailed(link.id, errorMessage);
          logger.error({ err: error, linkId: link.id }, "Falha ao enviar link.");
        }
      }
    }

    logger.warn("Reconectando sessao em 5 segundos...");
    await delay(5000);
  }
}

run().catch((error) => {
  logger.error({ err: error }, "Erro fatal no worker Baileys.");
  process.exit(1);
});
