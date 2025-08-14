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

/* ========= HARDCODED BRAND URLS (добавляйте сюда известные страницы) =========
 * Ключ — нормализованный бренд (lowercase, обрезанные пробелы).
 * Значения — список URL, присутствие которых считаем «да».
 */
const EXTRA_URLS = {
  // пример из вашего сообщения:
  "vizavi": [
    "https://www.wildberries.by/seller/488933"
  ],
  // примеры как можно добавлять:
  // "alani collection": ["https://www.wildberries.ru/brands/alani-collection"],
  // "t&n": ["https://www.ozon.ru/seller/XXXXX/"],  // если узнаете ID продавца на Ozon
};

/* ========= Sheets auth ========= */
function sheetsClientFromEnv() {
  if (!process.env.GOOGLE_CREDENTIALS) {
    throw new Error("Secret GOOGLE_CREDENTIALS is missing. Добавь JSON сервисного аккаунта (Secrets → Actions).");
  }
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
  const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
  return { sheets: google.sheets({ version: "v4", auth }), clientEmail: creds.client_email };
}

/* ========= Helpers ========= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normBrand(b) { return (b || "").trim().toLowerCase(); }

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

/* ========= DuckDuckGo fallback (укреплённый) ========= */
async function hasViaDDG(page, domain, brandVariant) {
  const queries = [
    `site:${domain} ${brandVariant}`,
    `site:${domain} "${brandVariant}"`,
  ];
  const endpoints = [
    (q) => `https://duckduckgo.com/html/?q=${encodeURIComponent(q)}`,
    (q) => `https://duckduckgo.com/lite/?q=${encodeURIComponent(q)}`
  ];
  for (const q of queries) {
    for (const makeUrl of endpoints) {
      const url = makeUrl(q);
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
        await page.waitForTimeout(800);
        const hasLink = await page.$$eval(`a[href*="${domain}"]`, els => els.length > 0);
        if (hasLink) return true;
        const html = await page.content();
        if (html.includes(domain)) return true;
      } catch {}
    }
  }
  return false;
}

/* ========= Brand variants ========= */
function brandVariants(brand) {
  const b = brand.trim();
  const set = new Set([b, b.toUpperCase(), b.toLowerCase()]);
  set.add(b.replace(/\s+/g, "-"));
  set.add(b.replace(/-/g, " "));
  return Array.from(set);
}

/* ========= Проверка «жёсткими» URL ========= */
async function hasByExtraUrls(page, brand) {
  const key = normBrand(brand);
  const urls = EXTRA_URLS[key];
  if (!urls || !urls.length) return false;

  for (const url of urls) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
      await dismissBanners(page);
      await page.waitForTimeout(800);

      // Общая эвристика «страница продавца/бренда с товарами»
      // 1) есть ли карточки/ссылки на товар
      const hasAnyProductLink = await page.$$eval('a[href*="/catalog/"], a[href*="/product/"], a[href*="/item/"]', els => els.length > 0);
      if (hasAnyProductLink) return true;

      // 2) текстовые признаки «продавец», «товары», «все товары»
      const txt = await page.evaluate(() => document.body?.innerText || "");
      if (/продавец/i.test(txt) && /товар/i.test(txt)) return true;

      // 3) если это WB селлер — часто есть блок со списком товаров
      const hasWbGrid = await page.$('.product-card, .product-card__wrapper, [data-card-index]') !== null;
      if (hasWbGrid) return true;
    } catch {
      // пробуем следующий URL
    }
    await page.waitForTimeout(400);
  }
  return false;
}

/* ========= Checks ========= */

// Wildberries (.ru и .by)
async function hasWB(page, brand) {
  // Сначала — прямые URL, если указаны
  const extra = await hasByExtraUrls(page, brand);
  if (extra) return true;

  // Пробуем поиск на .ru
  try {
    const urlRu = `https://www.wildberries.ru/catalog/0/search.aspx?search=${encodeURIComponent(brand)}`;
    await page.goto(urlRu, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(800);
    await autoScroll(page, 2500);

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

    const hasCardsRu = await page.$(".product-card, .product-card__wrapper, [data-card-index]") !== null;
    if (hasCardsRu) return true;
  } catch {}

  // Пробуем поиск на .by
  try {
    const urlBy = `https://www.wildberries.by/catalog/0/search.aspx?search=${encodeURIComponent(brand)}`;
    await page.goto(urlBy, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(800);
    await autoScroll(page, 2500);

    const hasCardsBy = await page.$(".product-card, .product-card__wrapper, [data-card-index]") !== null;
    if (hasCardsBy) return true;
  } catch {}

  // Фолбэк DDG по двум доменам
  for (const v of brandVariants(brand)) {
    if (await hasViaDDG(page, "wildberries.ru", v)) return true;
    if (await hasViaDDG(page, "wildberries.by", v)) return true;
  }

  // Явное «ничего не найдено» — не критично
  return false;
}

// Ozon
async function hasOzon(page, brand) {
  // прямые URL для бренда (если когда-то добавите)
  const extra = await hasByExtraUrls(page, brand);
  if (extra) return true;

  try {
    const url = `https://www.ozon.ru/search/?text=${encodeURIComponent(brand)}`;
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await dismissBanners(page);
    await page.waitForTimeout(1000);
    await autoScroll(page, 2500);

    const found = await page.evaluate(() => {
      try {
        const el = document.querySelector("#__PAGE_STATE__");
        if (!el) return null;
        const st = JSON.parse(el.textContent || "{}");
        const ws = st?.widgetStates || {};
        for (const k in ws) {
          if (k.includes("searchResults")) {
            const w = JSON.parse(ws[k]);
            if (Array.isArray(w
