import process from "node:process";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { google } from "googleapis";

puppeteer.use(StealthPlugin());

/* ========= ENV ========= */
const SHEET_ID   = process.env.SHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME || "Brands";
const MAX_PER_RUN = Number(process.env.MAX_PER_RUN || 300);
const USER_AGENT = process.env.USER_AGENT || "Mozilla/5.0";
const SLOW_MS    = Number(process.env.SLOW_MS || 800); // можно 600–1200

/* ========= Sheets auth ========= */
function sheetsClientFromEnv() {
  if (!process.env.GOOGLE_CREDENTIALS) {
    throw new Error("Secret GOOGLE_CREDENTIALS is missing.");
  }
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
  const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
  return { sheets: google.sheets({ version: "v4", auth }), clientEmail: creds.client_email };
}

/* ========= Helpers ========= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

function brandVariants(brand) {
  const b = (brand || "").trim();
  const set = new Set([b, b.toUpperCase(), b.toLowerCase()]);
  set.add(b.replace(/\s+/g, "-"));
  set.add(b.replace(/-/g, " "));
  // с кавычками — точное вхождение
  set.add(`"${b}"`);
  return Array.from(set);
}

async function getBrands(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2:A`,
  });
  const rows = res.data.values || [];
  return rows.map(r => (r[0] || "").toString().trim()).filter(Boolean);
}

async function writeResultsBatch(sheets, results) {
  if (!results.length) return;
  const data = results.map(({ rowIndex, wb, ozon, ym, ts }) => ({
    range: `${SHEET_NAME}!B${rowIndex}:E${rowIndex}`,
    values: [[ wb, ozon, ym, ts ]]
  }));
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "RAW", data }
  });
}

/* ========= Поисковая проверка (Bing + DuckDuckGo) =========
 * Успех = в результатах есть <a href> с нужным доменом.
 */
async function hasInSearch(page, domain, brand) {
  const variants = brandVariants(brand);

  const bing = (q) =>
    `https://www.bing.com/search?q=${encodeURIComponent(q)}&setlang=ru-ru`;
  const ddgHtml = (q) =>
    `https://duckduckgo.com/html/?q=${encodeURIComponent(q)}`;
  const ddgLite = (q) =>
    `https://duckduckgo.com/lite/?q=${encodeURIComponent(q)}`;

  const engines = [
    (q) => bing(`site:${domain} ${q}`),
    (q) => ddgHtml(`site:${domain} ${q}`),
    (q) => ddgLite(`site:${domain} ${q}`),
  ];

  for (const v of variants) {
    for (const mk of engines) {
      const url = mk(v);
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
        await page.setExtraHTTPHeaders({ "Accept-Language": "ru-RU,ru;q=0.9" });
        await page.waitForTimeout(500);

        // есть ссылка на нужный домен?
        const found = await page.$$eval(`a[href*="${domain}"]`, els => els.length > 0);
        if (found) return true;

        // на крайний случай — проверим HTML на наличие домена (обход нестандартной верстки)
        const html = await page.content();
        if (html.includes(domain)) return true;
      } catch {
        // пробуем следующий движок/вариант
      }
      await page.waitForTimeout(200);
    }
  }
  return false;
}

/* ========= Основная логика ========= */
(async () => {
  if (!SHEET_ID) throw new Error("SHEET_ID is not set.");
  const { sheets, clientEmail } = sheetsClientFromEnv();

  // preflight: доступ к таблице
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!E2`,
    valueInputOption: "RAW",
    requestBody: { values: [[nowIso()]] }
  });
  console.log(`Preflight OK: лист "${SHEET_NAME}" доступен.`);

  const brands = await getBrands(sheets);
  if (!brands.length) {
    console.log("Нет брендов в колонке A (начиная с A2).");
    return;
  }

  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-blink-features=AutomationControlled"
    ]
  });
  const page = await browser.newPage();
  await page.setUserAgent(USER_AGENT);
  await page.setViewport({ width: 1366, height: 900 });

  const DOMAINS = {
    wb: ["wildberries.ru", "wildberries.by"],
    ozon: ["ozon.ru", "ozon.by"],
    ym: ["market.yandex.ru"]
  };

  const total = Math.min(brands.length, MAX_PER_RUN);
  const out = [];

  for (let i = 0; i < total; i++) {
    const brand = brands[i];
    const rowIndex = i + 2;

    try {
      let wbOk = false, ozOk = false, ymOk = false;

      // WB: достаточно найти на любом из доменов
      for (const d of DOMAINS.wb) {
        if (await hasInSearch(page, d, brand)) { wbOk = true; break; }
      }
      await sleep(SLOW_MS);

      for (const d of DOMAINS.ozon) {
        if (await hasInSearch(page, d, brand)) { ozOk = true; break; }
      }
      await sleep(SLOW_MS);

      for (const d of DOMAINS.ym) {
        if (await hasInSearch(page, d, brand)) { ymOk = true; break; }
      }

      const wbVal = wbOk ? "да" : "нет";
      const ozVal = ozOk ? "да" : "нет";
      const ymVal = ymOk ? "да" : "нет";
      const ts = nowIso();

      out.push({ rowIndex, wb: wbVal, ozon: ozVal, ym: ymVal, ts });
      console.log(`[${rowIndex}] ${brand}: WB=${wbVal}, Ozon=${ozVal}, YM=${ymVal}`);
    } catch (e) {
      console.warn(`[${rowIndex}] ${brand}: error ${e.message}`);
    }
  }

  if (out.length) {
    await writeResultsBatch(sheets, out);
    console.log(`Готово: записано ${out.length} строк.`);
  }

  await browser.close();
})().catch(err => {
  console.error("FATAL:", err.message);
});
