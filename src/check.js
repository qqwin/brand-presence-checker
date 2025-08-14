import process from "node:process";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { google } from "googleapis";

puppeteer.use(StealthPlugin());

const SHEET_ID   = process.env.SHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME || "Brands";
const MAX_PER_RUN = Number(process.env.MAX_PER_RUN || 600);
const USER_AGENT = process.env.USER_AGENT || "Mozilla/5.0";
const SLOW_MS    = Number(process.env.SLOW_MS || 1000);

function sheetsClientFromEnv() {
  if (!process.env.GOOGLE_CREDENTIALS) {
    throw new Error("Secret GOOGLE_CREDENTIALS is missing. Добавь JSON сервисного аккаунта в Settings → Secrets → Actions.");
  }
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
  const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
  return { sheets: google.sheets({ version: "v4", auth }), clientEmail: creds.client_email };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function preflight(sheets, clientEmail) {
  if (!SHEET_ID) throw new Error("SHEET_ID is not set. Добавь секрет SHEET_ID в репозиторий.");
  try {
    await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SHEET_NAME}!A2:A` });
  } catch (e) {
    const msg = String(e);
    if (msg.includes("The caller does not have permission")) {
      throw new Error(`Нет прав на таблицу. Открой Google Sheet → “Поделиться” → добавь ${clientEmail} как Редактор.`);
    }
    if (msg.includes("Requested entity was not found")) {
      throw new Error("Неверный SHEET_ID или лист SHEET_NAME. Проверь ID и имя листа (по умолчанию 'Brands').");
    }
    throw e;
  }
  const ts = new Date().toISOString();
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!E2`,
    valueInputOption: "RAW",
    requestBody: { values: [[ts]] }
  });
  console.log(`Preflight OK: можем читать и писать в лист "${SHEET_NAME}".`);
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

async function hasWB(page, brand) {
  const q = encodeURIComponent(brand);
  const url = `https://www.wildberries.ru/catalog/0/search.aspx?search=${q}`;
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
    await page.waitForTimeout(1200);
    const stateFound = await page.evaluate(() => {
      try {
        const script = [...document.querySelectorAll("script")].find(s => s.textContent && s.textContent.includes("window.__PRELOADED_STATE__"));
        if (!script) return null;
        const txt = script.textContent;
        const start = txt.indexOf("window.__PRELOADED_STATE__ = ");
        if (start === -1) return null;
        const cut = txt.slice(start + "window.__PRELOADED_STATE__ = ".length);
        const end = cut.indexOf(";</");
        const jsonStr = (end === -1 ? cut : cut.slice(0, end)).trim();
        const data = JSON.parse(jsonStr);
        const products =
          data?.products?.items ||
          data?.search?.products ||
          data?.catalog?.products || [];
        if (Array.isArray(products) && products.length > 0) return true;
        return false;
      } catch { return null; }
    });
    if (stateFound === true) return true;
    if (stateFound === false) return false;
    const hasCards = await page.$(".product-card, .product-card__wrapper, [data-card-index]") !== null;
    return !!hasCards;
  } catch {
    return false;
  }
}

async function hasOzon(page, brand) {
  const q = encodeURIComponent(brand);
  const url = `https://www.ozon.ru/search/?text=${q}`;
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
    await page.waitForTimeout(1500);
    const found = await page.evaluate(() => {
      try {
        const el = document.querySelector('#__PAGE_STATE__');
        if (!el) return null;
        const st = JSON.parse(el.textContent || "{}");
        const ws = st?.widgetStates || {};
        for (const k in ws) {
          if (k.includes("searchResults")) {
            const w = JSON.parse(ws[k]);
            if (Array.isArray(w?.items) && w.items.length > 0) return true;
          }
        }
        return false;
      } catch { return null; }
    });
    if (found === true) return true;
    if (found === false) return false;
    const hasCards = await page.$('[data-widget="searchResultsV2"], [data-widget="searchResults"]') !== null;
    return !!hasCards;
  } catch {
    return false;
  }
}

async function hasYandexMarket(page, brand) {
  const q = encodeURIComponent(brand);
  const url = `https://market.yandex.ru/search?text=${q}`;
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
    await page.waitForTimeout(1800);
    const hasZone = await page.$('[data-zone-name="SearchResults"]') !== null;
    if (hasZone) {
      const hasItems = await page.$$eval(
        '[data-zone-name="SearchResults"] [data-autotest-id="product-snippet"], [data-auto="snippet-cell"]',
        els => els.length > 0
      );
      if (hasItems) return true;
    }
    const text = await page.evaluate(() => document.body.innerText || "");
    if (/ничего не нашлось/i.test(text)) return false;
    const anyLinks = await page.$$eval('a[href*="market.yandex.ru"]', els => els.length > 0);
    return !!anyLinks;
  } catch {
    return false;
  }
}

(async () => {
  const { sheets, clientEmail } = sheetsClientFromEnv();
  await preflight(sheets, clientEmail);

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

  const total = Math.min(brands.length, MAX_PER_RUN);
  const out = [];

  for (let i = 0; i < total; i++) {
    const brand = brands[i];
    const rowIndex = i + 2;

    try {
      const wb = await hasWB(page, brand);
      await sleep(SLOW_MS);
      const oz = await hasOzon(page, brand);
      await sleep(SLOW_MS);
      const ym = await hasYandexMarket(page, brand);

      const wbVal = wb ? "да" : "нет";
      const ozVal = oz ? "да" : "нет";
      const ymVal = ym ? "да" : "нет";
      const ts = new Date().toISOString();

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
