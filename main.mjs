import process from "node:process";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { google } from "googleapis";

puppeteer.use(StealthPlugin());

/* ========= ENV ========= */
const SHEET_ID        = process.env.SHEET_ID;
const SHEET_NAME      = process.env.SHEET_NAME || "Brands";
const MAX_PER_RUN     = Number(process.env.MAX_PER_RUN || 300);
const USER_AGENT      = process.env.USER_AGENT || "Mozilla/5.0";
const SLOW_MS         = Number(process.env.SLOW_MS || 900);
const BATCH_PER_PROXY = Number(process.env.BATCH_PER_PROXY || 60);
const PROXIES_RAW     = (process.env.PROXIES || "").trim();
const PROXIES         = PROXIES_RAW ? PROXIES_RAW.split(/\s*,\s*/).filter(Boolean) : [];

function pick(arr, i) { return arr[i % arr.length]; }

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

// прокрутка, чтобы догрузились карточки
async function autoScroll(page, maxMs = 4500) {
  const start = Date.now();
  try {
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let total = 0;
        const step = 600;
        const timer = setInterval(() => {
          const root = document.scrollingElement || document.documentElement;
          const sh = root.scrollHeight;
          window.scrollBy(0, step);
          total += step;
          if (total >= sh - window.innerHeight - 50) {
            clearInterval(timer);
            resolve();
          }
        }, 120);
      });
    });
  } catch {}
  while (Date.now() - start < maxMs) {
    await page.waitForTimeout(200);
  }
}

// скрыть баннеры cookie/гео
async function dismissBanners(page) {
  const selectors = [
    'button[aria-label="Принять"]',
    'button:has-text("Принять")',
    'button:has-text("Согласен")',
    'button:has-text("I agree")',
    '[data-testid="cookies-popup"] button',
    '[data-auto="region-confirm-button"]',
    'button:has-text("Понятно")',
  ];
  for (const s of selectors) {
    try {
      const el = await page.$(s);
      if (el) { await el.click({ delay: 40 }); await page.waitForTimeout(150); }
    } catch {}
  }
}

async function setupPage(page) {
  await page.setUserAgent(USER_AGENT);
  await page.setExtraHTTPHeaders({ "Accept-Language": "ru-RU,ru;q=0.9" });
  await page.emulateTimezone("Europe/Moscow");
  await page.setViewport({ width: 1366, height: 900 });
}

/* ========= Checks (прямой парсинг) ========= */

// Wildberries (.ru и .by)
async function hasWB(page, brand) {
  const queries = [
    `https://www.wildberries.ru/catalog/0/search.aspx?search=${encodeURIComponent(brand)}`,
    `https://www.wildberries.by/catalog/0/search.aspx?search=${encodeURIComponent(brand)}`
  ];
  for (const url of queries) {
    try {
      await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
      await dismissBanners(page);
      await page.waitForTimeout(800);
      await autoScroll(page, 3000);

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
      if (typeof count === "number" && count > 0) return true;

      // 2) карточки в DOM
      const hasCards = await page.$(".product-card, .product-card__wrapper, [data-card-index]") !== null;
      if (hasCards) return true;

      // 3) явный текст про отсутствие
      const nothing = await page.evaluate(() => /ничего не найдено|ничего не найден/i.test(document.body?.innerText || ""));
      if (nothing) continue;
    } catch {
      // пробуем следующий домен
    }
  }
  return false;
}

// Ozon
async function hasOzon(page, brand) {
  const url = `https://www.ozon.ru/search/?text=${encodeURIComponent(brand)}`;
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(1000);
    await autoScroll(page, 3000);

    // 1) __PAGE_STATE__ → items
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

    // 2) контейнеры/карточки
    const hasResults = await page.$('[data-widget="searchResultsV2"], [data-widget="searchResults"]') !== null;
    if (hasResults) {
      const itemsCount = await page.$$eval(
        '[data-widget="searchResultsV2"] a, [data-widget="searchResults"] a',
        els => els.length
      );
      if (itemsCount > 0) return true;
    }

    // 3) явное "ничего не найдено"
    const nothing = await page.evaluate(() => /ничего не найдено/i.test(document.body?.innerText || ""));
    if (nothing) return false;
  } catch {}
  return false;
}

// Яндекс Маркет (фикс региона Москва lr=213)
async function hasYandexMarket(page, brand) {
  const url = `https://market.yandex.ru/search?text=${encodeURIComponent(brand)}&lr=213`;
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(1200);
    await autoScroll(page, 3200);

    // 1) зона + карточки
    const hasZone = await page.$('[data-zone-name="SearchResults"]') !== null;
    if (hasZone) {
      const hasItems = await page.$$eval(
        '[data-zone-name="SearchResults"] [data-autotest-id="product-snippet"], [data-auto="snippet-cell"]',
        els => els.length > 0
      );
      if (hasItems) return true;
    }

    // 2) счётчик
    const hasCounter = await page.evaluate(() => {
      const txt = document.body?.innerText || "";
      const m = txt.match(/Найдено\s+(\d[\d\s]*)\s+товар/i);
      if (!m) return null;
      const n = parseInt(m[1].replace(/\s+/g, ""), 10);
      return Number.isFinite(n) ? n > 0 : null;
    });
    if (hasCounter === true) return true;

    // 3) явное отсутствие
    const nothing = await page.evaluate(() => /ничего не нашлось/i.test(document.body?.innerText || ""));
    if (nothing) return false;
  } catch {}
  return false;
}

/* ========= Launch / Proxy handling ========= */
async function launchBrowserWithProxy(proxyUrl = null) {
  const args = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled"
  ];
  if (proxyUrl) args.push(`--proxy-server=${proxyUrl}`);
  const browser = await puppeteer.launch({ headless: "new", args });
  return browser;
}

/* ========= Main ========= */
(async () => {
  if (!SHEET_ID) throw new Error("SHEET_ID is not set.");
  const { sheets, clientEmail } = sheetsClientFromEnv();

  // Быстрая проверка записи
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

  // Разобьём на батчи, чтобы перезапускать браузер и (по возможности) ротацию прокси
  const total = Math.min(brands.length, MAX_PER_RUN);
  let processed = 0;
  let batchIndex = 0;
  const out = [];

  while (processed < total) {
    const start = processed;
    const end = Math.min(total, start + BATCH_PER_PROXY);
    const proxy = PROXIES.length ? pick(PROXIES, batchIndex) : null;

    console.log(`Запуск браузера. Прокси: ${proxy || "без прокси"}. Бренды ${start + 1}–${end}.`);

    const browser = await launchBrowserWithProxy(proxy);
    const page = await browser.newPage();
    await setupPage(page);

    for (let i = start; i < end; i++) {
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
        const ts = nowIso();

        out.push({ rowIndex, wb: wbVal, ozon: ozVal, ym: ymVal, ts });
        console.log(`[${rowIndex}] ${brand}: WB=${wbVal}, Ozon=${ozVal}, YM=${ymVal}`);
      } catch (e) {
        console.warn(`[${rowIndex}] ${brand}: error ${e.message}`);
      }
    }

    await browser.close();
    processed = end;
    batchIndex++;
  }

  if (out.length) {
    await writeResultsBatch(sheets, out);
    console.log(`Готово: записано ${out.length} строк.`);
  }
})().catch(err => {
  console.error("FATAL:", err.message);
});
