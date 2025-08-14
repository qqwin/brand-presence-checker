import process from "node:process";
import { google } from "googleapis";

/* ===== Config from env ===== */
const SHEET_ID   = process.env.SHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME || "Brands";
const MAX_PER_RUN = Number(process.env.MAX_PER_RUN || 600);
const TIMEOUT_MS  = Number(process.env.TIMEOUT_MS || 18000);
const USER_AGENT  = process.env.USER_AGENT || "Mozilla/5.0";

/* ===== Utilities ===== */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

function withTimeout(ms) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  return { signal: ctrl.signal, cancel: () => clearTimeout(t) };
}

async function httpGet(url) {
  const { signal, cancel } = withTimeout(TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9" },
      signal
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text };
  } finally {
    cancel();
  }
}

/* ===== Google Sheets ===== */
function sheetsClientFromEnv() {
  if (!process.env.GOOGLE_CREDENTIALS) {
    throw new Error("Secret GOOGLE_CREDENTIALS is missing.");
  }
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const auth = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  return google.sheets({ version: "v4", auth });
}

async function getBrands(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2:A`,
  });
  const rows = res.data.values || [];
  return rows.map(r => (r[0] || "").toString().trim()).filter(Boolean);
}

async function writeResultsBatch(sheets, rows) {
  if (!rows.length) return;
  const data = rows.map(({ rowIndex, wb, ozon, ym, ts }) => ({
    range: `${SHEET_NAME}!B${rowIndex}:E${rowIndex}`,
    values: [[wb, ozon, ym, ts]]
  }));
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "RAW", data }
  });
}

/* ===== Brand variants for search ===== */
function brandVariants(brand) {
  const b = (brand || "").trim();
  const set = new Set([b, `"${b}"`, b.toUpperCase(), b.toLowerCase()]);
  set.add(b.replace(/\s+/g, "-"));
  set.add(`"${b.replace(/\s+/g, "-")}"`);
  return Array.from(set);
}

/* ===== WB via public JSON API (no key) =====
   Пытаемся получить JSON с товарами. Если есть товары — "да".
   Пробуем несколько вариантов эндпоинтов.
*/
async function hasWB_API(brand) {
  const q = encodeURIComponent(brand);
  const urls = [
    // v4/v5 иногда меняются — пробуем оба, разные параметры, ru/by
    `https://search.wb.ru/exactmatch/ru/common/v4/search?query=${q}&resultset=catalog&limit=1&page=1`,
    `https://search.wb.ru/exactmatch/ru/common/v5/search?query=${q}&resultset=catalog&limit=1&page=1`,
    `https://search.wb.ru/exactmatch/ru/common/v4/search?xquery=${q}&resultset=catalog&limit=1&page=1`,
    `https://search.wb.ru/exactmatch/ru/common/v5/search?xquery=${q}&resultset=catalog&limit=1&page=1`,
    `https://search.wb.ru/exactmatch/ru/common/v4/search?query=${q}&page=1`,
    `https://search.wb.ru/exactmatch/ru/common/v5/search?query=${q}&page=1`,
    // BY
    `https://search.wb.ru/exactmatch/by/common/v5/search?query=${q}&resultset=catalog&limit=1&page=1`
  ];
  for (const url of urls) {
    try {
      const { ok, text } = await httpGet(url);
      if (!ok || !text) continue;
      // иногда возвращает JSON5-подобное; попробуем безопасный parse
      const cleaned = text.replace(/,\s*}/g, "}").replace(/,\s*]/g, "]");
      let data = null;
      try { data = JSON.parse(cleaned); } catch {}
      if (!data) continue;

      // разные структуры — пытаемся достать количество/список
      const products =
        data?.data?.products ||
        data?.data?.catalog?.products ||
        data?.data?.items ||
        data?.products ||
        [];
      const total =
        (typeof data?.data?.total === "number" && data.data.total) ||
        (Array.isArray(products) ? products.length : 0);

      if (total > 0) return true;
      if (Array.isArray(products) && products.length > 0) return true;
    } catch {
      // пробуем следующий URL
    }
  }
  return false;
}

/* ===== Generic search via DuckDuckGo HTML =====
   Успех: есть хотя бы одна ссылка <a href> c нужным доменом.
*/
async function hasBySearch(domain, brand) {
  const mk = (q) => `https://duckduckgo.com/html/?q=${encodeURIComponent(`site:${domain} ${q}`)}`;
  for (const v of brandVariants(brand)) {
    const url = mk(v);
    try {
      const { ok, text } = await httpGet(url);
      if (!ok || !text) continue;
      // очень просто: есть ли в выдаче href на нужный домен?
      if (text.includes(`href="https://${domain}`) || text.includes(`href="http://${domain}`)) {
        return true;
      }
      // запасной: просто наличие домена в HTML
      if (text.includes(domain)) return true;
    } catch {
      // идём к следующему варианту
    }
    await sleep(200);
  }
  return false;
}

/* ===== High-level checks ===== */
async function checkWB(brand) {
  // 1) пробуем нативный JSON API
  if (await hasWB_API(brand)) return true;
  // 2) фолбэк: поисковик (по двум доменам)
  if (await hasBySearch("wildberries.ru", brand)) return true;
  if (await hasBySearch("wildberries.by", brand)) return true;
  return false;
}

async function checkOzon(brand) {
  return await hasBySearch("ozon.ru", brand);
}

async function checkYandexMarket(brand) {
  return await hasBySearch("market.yandex.ru", brand);
}

/* ===== Main ===== */
(async () => {
  if (!SHEET_ID) throw new Error("SHEET_ID is not set.");

  const sheets = sheetsClientFromEnv();

  // Быстрая проверка записи
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!E2`,
    valueInputOption: "RAW",
    requestBody: { values: [[nowIso()]] }
  });
  console.log(`Preflight OK: лист "${SHEET_NAME}" доступен для чтения/записи.`);

  const brands = await getBrands(sheets);
  if (!brands.length) {
    console.log("Нет брендов в колонке A (начиная с A2).");
    return;
  }

  const total = Math.min(brands.length, MAX_PER_RUN);
  const out = [];

  for (let i = 0; i < total; i++) {
    const brand = brands[i];
    const rowIndex = i + 2;

    try {
      const wb = await checkWB(brand);
      const oz = await checkOzon(brand);
      const ym = await checkYandexMarket(brand);

      const wbVal = wb ? "да" : "нет";
      const ozVal = oz ? "да" : "нет";
      const ymVal = ym ? "да" : "нет";
      const ts = nowIso();

      out.push({ rowIndex, wb: wbVal, ozon: ozVal, ym: ymVal, ts });
      console.log(`[${rowIndex}] ${brand}: WB=${wbVal}, Ozon=${ozVal}, YM=${ymVal}`);
    } catch (e) {
      console.warn(`[${rowIndex}] ${brand}: error ${e.message}`);
    }

    // лёгкая пауза чтобы не долбить DDG слишком быстро
    await sleep(300);
  }

  if (out.length) {
    await writeResultsBatch(sheets, out);
    console.log(`Готово: записано ${out.length} строк.`);
  }
})().catch(err => {
  console.error("FATAL:", err.message);
});
