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
const SLOW_MS    = Number(process.env.SLOW_MS || 1200);

/* ========= Sheets auth ========= */
function sheetsClientFromEnv() {
  if (!process.env.GOOGLE_CREDENTIALS) {
    throw new Error("Secret GOOGLE_CREDENTIALS is missing. Добавь JSON сервисного аккаунта в Settings → Secrets → Actions.");
  }
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
  const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
  return { sheets: google.sheets({ version: "v4", auth }), clientEmail: creds.client_email };
}

/* ========= Helpers ========= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

// плавная прокрутка, чтобы догрузился список
async function autoScroll(page, maxMs = 4000) {
  const start = Date.now();
  await page.evaluate(async () => {
    await new Promise((resolve) => {
      let total = 0;
      const step = 500;
      const timer = setInterval(() => {
        const root = document.scrollingElement || document.documentElement;
        const sh = root.scrollHeight;
        window.scrollBy(0, step);
        total += step;
        if (total >= sh - window.innerHeight - 50) {
          clearInterval(timer);
          resolve();
        }
      }, 100);
    });
  });
  while (Date.now() - start < maxMs) {
    await page.waitForTimeout(200);
  }
}

// закрыть типовые баннеры cookie/гео
async function dismissBanners(page) {
  const selectors = [
    'button[aria-label="Принять"]',
    'button:has-text("Принять")',
    'button:has-text("Согласен")',
    'button:has-text("I agree")',
    '[data-testid="cookies-popup"] button',
    '[data-auto="region-confirm-button"]',
  ];
  for (const s of selectors) {
    try {
      const el = await page.$(s);
      if (el) { await el.click({ delay: 50 }); await page.waitForTimeout(200); }
    } catch {}
  }
}

// общая подготовка страницы
async function setupPage(page) {
  await page.setUserAgent(USER_AGENT);
  await page.setExtraHTTPHeaders({ "Accept-Language": "ru-RU,ru;q=0.9" });
  await page.emulateTimezone("Europe/Moscow"); // ближе к целевому рынку
  await page.setViewport({ width: 1366, height: 900 });
}

/* ========= Checks ========= */

// Wildberries
async function hasWB(page, brand) {
  const q = encodeURIComponent(brand);
  const url = `https://www.wildberries.ru/catalog/0/search.aspx?search=${q}`;
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(800);
    await autoScroll(page, 2500);

    // 1) preloaded state
    const count = await page.evaluate(() => {
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
        const products = data?.products?.items || data?.search?.products || data?.catalog?.products || [];
        return Array.isArray(products) ? products.length : null;
      } catch { return null; }
    });
    if (typeof count === "number") return count > 0;

    // 2) карточки в DOM
    const hasCards = await page.$(".product-card, .product-card__wrapper, [data-card-index]") !== null;
    if (hasCards) return true;

    // 3) явное "ничего не найдено"
    const nothing = await page.evaluate(() => /ничего не найдено/i.test(document.body.innerText || ""));
    return !nothing ? false : false;
  } catch {
    return false;
  }
}

// Ozon
async function hasOzon(page, brand) {
  const q = encodeURIComponent(brand);
  const url = `https://www.ozon.ru/search/?text=${q}`;
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(1000);
    await autoScroll(page, 2500);

    // 1) __PAGE_STATE__
    const found = await page.evaluate(() => {
      try {
        const el = document.querySelector("#__PAGE_STATE__");
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
    if (found === false) {
      // иногда state пустой, но карточки есть
    }

    // 2) контейнеры/карточки
    const hasResults = await page.$('[data-widget="searchResultsV2"], [data-widget="searchResults"]') !== null;
    if (hasResults) {
      const itemsCount = await page.$$eval('[data-widget="searchResultsV2"] a, [data-widget="searchResults"] a', els => els.length);
      if (itemsCount > 0) return true;
    }

    // 3) явное отсутствие
    const nothing = await page.evaluate(() => /ничего не найдено/i.test(document.body.innerText || ""));
    return !nothing ? false : false;
  } catch {
    return false;
  }
}

// Яндекс Маркет (фикс региона Москва lr=213)
async function hasYandexMarket(page, brand) {
  const q = encodeURIComponent(brand);
  const url = `https://market.yandex.ru/search?text=${q}&lr=213`;
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(1200);
    await autoScroll(page, 3000);

    // 1) основная зона + карточки
    const hasZone = await page.$('[data-zone-name="SearchResults"]') !== null;
    if (hasZone) {
      const hasItems = await page.$$eval(
        '[data-zone-name="SearchResults"] [data-autotest-id="product-snippet"], [data-auto="snippet-cell"]',
        els => els.length > 0
      );
      if (hasItems) return true;
    }

    // 2) счётчик результатов (если есть)
    const hasCounter = await page.evaluate(() => {
      const txt = document.body.innerText || "";
      const m = txt.match(/Найдено\s+(\d[\d\s]*)\s+товар/i);
      if (!m) return null;
      const n = parseInt(m[1].replace(/\s+/g, ""), 10);
      return Number.isFinite(n) ? n > 0 : null;
    });
    if (hasCounter === true) return true;

    // 3) явное "ничего не нашлось"
    const nothing = await page.evaluate(() => /ничего не нашлось/i.test(document.body.innerText || ""));
    return !nothing ? false : false;
  } catch {
    return false;
  }
}

/* ========= Main ========= */
(async () => {
  if (!SHEET_ID) throw new Error("SHEET_ID is not set. Добавь секрет SHEET_ID.");
  const { sheets, clientEmail } = sheetsClientFromEnv();

  // preflight: права/доступ
  try {
    await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${SHEET_NAME}!A2:A` });
  } catch (e) {
    const msg = String(e);
    if (msg.includes("The caller does not have permission")) {
      throw new Error(`Нет прав на таблицу. Поделитесь таблицей с ${clientEmail} (Editor).`);
    }
    if (msg.includes("Requested entity was not found")) {
      throw new Error("Неверный SHEET_ID или имя листа (SHEET_NAME).");
    }
    throw e;
  }

  // тестовая запись времени в E2 — заодно проверка записи
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!E2`,
    valueInputOption: "RAW",
    requestBody: { values: [[new Date().toISOString()]] }
  });
  console.log(`Preflight OK: лист "${SHEET_NAME}" доступен для чтения/записи.`);

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
  await setupPage(page);

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
