// ======================================================
// ESTADO ÚNICO DA USINA (FONTE DA VERDADE NO FRONT)
// ======================================================
let PLANT_STATE = {
  name: "Usina Acopiara",
  rated_power_kwp: 2070.0,
  active_power_kw: 985.9,
  capacity_percent: 47.6,
  inverter_total: 8,
  inverter_online: 7,
  pr_percent: 47.6
};

// ======================================================
// CONFIG (ONLINE/OFFLINE)
// ======================================================
const INVERTER_ONLINE_AFTER_MS = 8 * 60 * 1000; // 8 min (ajuste aqui)

// ======================================================
// FUNÇÕES AUXILIARES
// ======================================================
function asNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const s = typeof value === "string" ? value.replace(",", ".").trim() : value;
  const parsed = Number(s);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatNumberPtBR(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("pt-BR").format(n);
}

function formatKwhPtBR(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${formatNumberPtBR(n)} kWh`;
}

function buildLastNDaysLabels(n) {
  const labels = [];
  const today = new Date();
  for (let i = n - 1; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(today.getDate() - i);
    const dd = String(d.getDate()).padStart(2, "0");
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    labels.push(`${dd}/${mm}`);
  }
  return labels;
}

function looksLikeDayOnlyLabel(label) {
  const s = String(label ?? "").trim();
  if (!s) return false;
  if (s.includes("/") || s.includes("-")) return false;
  return /^\d{1,2}$/.test(s);
}

function hasDuplicateLabels(labels) {
  const set = new Set();
  for (const l of labels) {
    const key = String(l);
    if (set.has(key)) return true;
    set.add(key);
  }
  return false;
}

function valueOrDash(v) {
  return v === null || v === undefined || v === "" ? "—" : v;
}

function fmtDatePtBR(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d.getTime()) ? "—" : d.toLocaleString("pt-BR");
}

function numFixedOrDash(v, digits = 1) {
  // ✅ IMPORTANT: 0 deve aparecer como "0.0", não "—"
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  return Number.isFinite(n) ? n.toFixed(digits) : "—";
}

function normalizePercentMaybe(v) {
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  if (!Number.isFinite(n)) return null;
  if (n <= 1.0) return n * 100;
  return n;
}

function fmtAmp(v) {
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(2)} A`;
}

// ======================================================
// ✅ HELPERS NUMÉRICOS PARA MENSAL (UNIDADE + OUTLIER)
// ======================================================
function median(arr) {
  const a = arr.filter(x => Number.isFinite(x)).slice().sort((x, y) => x - y);
  if (!a.length) return 0;
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

function p95(arr) {
  const a = arr.filter(x => Number.isFinite(x)).slice().sort((x, y) => x - y);
  if (!a.length) return 0;
  const idx = Math.max(0, Math.min(a.length - 1, Math.floor((a.length - 1) * 0.95)));
  return a[idx];
}

/**
 * Converte Wh->kWh se detectar escala absurda.
 * - Para 30 dias de uma usina ~2MW, MTD em kWh não deveria ir pra milhões.
 */
function maybeConvertWhToKwh(dailyArr, mtdArr) {
  const maxCum = Math.max(...(mtdArr || []), 0);
  const looksLikeWh = maxCum > 500000; // gatilho conservador
  if (!looksLikeWh) return { daily: dailyArr, mtd: mtdArr, converted: false };
  return {
    daily: dailyArr.map(v => v / 1000),
    mtd: mtdArr.map(v => v / 1000),
    converted: true
  };
}

/**
 * Trata outliers: se um dia é MUITO maior que o normal, capamos pra não destruir o gráfico.
 * Regra: se v > max(mediana*25, p95*4) => cap = max(mediana*10, p95*1.5)
 */
function capMonthlyOutliers(dailyArr) {
  const daily = dailyArr.map(v => Number(v) || 0);

  const med = median(daily);
  const q95 = p95(daily);

  const spikeThreshold = Math.max(med * 25, q95 * 4);
  const capValue = Math.max(med * 10, q95 * 1.5);

  let changed = false;
  const capped = daily.map(v => {
    if (med <= 0 && q95 <= 0) return v;
    if (v > spikeThreshold && capValue > 0) {
      changed = true;
      return capValue;
    }
    return v;
  });

  return { daily: capped, changed, med, q95, spikeThreshold, capValue };
}

// ======================================================
// ✅ NORMALIZA PAYLOAD DIÁRIO (00:00 até último dado do dia)
// - Filtra apenas pontos do dia atual quando houver timestamp por ponto
// - Preenche faltas com 0 para evitar buracos visuais
// ======================================================
function normalizeDailyPayload(payload) {
  if (!payload) return payload;

  const labelsRaw = Array.isArray(payload.labels) ? payload.labels.slice() : [];

  const powerRaw =
    Array.isArray(payload.activePower) ? payload.activePower.slice() :
    Array.isArray(payload.active_power_kw) ? payload.active_power_kw.slice() :
    Array.isArray(payload.power_kw) ? payload.power_kw.slice() :
    [];

  const irrRaw =
    Array.isArray(payload.irradiance) ? payload.irradiance.slice() :
    Array.isArray(payload.irradiance_wm2) ? payload.irradiance_wm2.slice() :
    [];

  if (!labelsRaw.length) return payload;

  // timestamp por ponto (se existir), para filtrar estritamente o DIA ATUAL
  const pointTsRaw =
    Array.isArray(payload.timestamps) ? payload.timestamps :
    Array.isArray(payload.ts) ? payload.ts :
    Array.isArray(payload.point_timestamps) ? payload.point_timestamps :
    Array.isArray(payload.time) ? payload.time :
    null;

  const hasPointTimestamps = Array.isArray(pointTsRaw) && pointTsRaw.length > 0;

  // "HH:mm" -> minutos desde 00:00
  const toMin = (hhmm) => {
    const m = String(hhmm || "").trim().match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return null;
    const hh = Number(m[1]), mm = Number(m[2]);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
    return hh * 60 + mm;
  };

  const dateKeyInSaoPaulo = (d) => {
    if (!(d instanceof Date) || Number.isNaN(d.getTime())) return null;
    return d.toLocaleDateString("en-CA", { timeZone: "America/Sao_Paulo" });
  };

  const todayKeySP = new Date().toLocaleDateString("en-CA", { timeZone: "America/Sao_Paulo" });

  const points = [];
  for (let i = 0; i < labelsRaw.length; i++) {
    const minute = toMin(labelsRaw[i]);
    if (minute == null) continue;

    if (hasPointTimestamps) {
      const ts = pointTsRaw[i];
      const d = ts ? new Date(ts) : null;
      const key = dateKeyInSaoPaulo(d);
      if (!key || key !== todayKeySP) continue;
    }

    points.push({
      minute,
      power: powerRaw[i] != null ? asNumber(powerRaw[i], 0) : 0,
      irr: irrRaw[i] != null ? asNumber(irrRaw[i], 0) : 0
    });
  }

  if (!points.length) {
    return {
      ...payload,
      labels: [],
      activePower: [],
      irradiance: []
    };
  }

  const mins = points.map(p => p.minute).sort((a, b) => a - b);

  // detecta o passo (1,5,10,15...) olhando o menor diff positivo
  let step = 5; // fallback
  if (mins.length >= 3) {
    const diffs = [];
    for (let i = 1; i < mins.length; i++) {
      const d = mins[i] - mins[i - 1];
      if (d > 0 && d <= 60) diffs.push(d);
    }
    if (diffs.length) step = Math.max(1, Math.min(...diffs));
  }

  const mapP = new Map();
  const mapI = new Map();
  points.forEach(p => {
    mapP.set(p.minute, p.power);
    mapI.set(p.minute, p.irr);
  });

  // começa SEMPRE em 00:00 e termina no último minuto que chegou dado hoje
  const lastMin = Math.max(...mins);

  const labels = [];
  const activePower = [];
  const irradiance = [];

  for (let m = 0; m <= lastMin; m += step) {
    const hh = String(Math.floor(m / 60)).padStart(2, "0");
    const mm = String(m % 60).padStart(2, "0");
    labels.push(`${hh}:${mm}`);

    // sem buraco visual: minutos faltantes viram 0
    activePower.push(mapP.has(m) ? mapP.get(m) : 0);
    irradiance.push(mapI.has(m) ? mapI.get(m) : 0);
  }

  return {
    ...payload,
    labels,
    activePower,
    irradiance
  };
}


// ======================================================
// MOCK STRINGS (pra não depender da API)
// ======================================================
function buildMockStringsPayload(inverterId, count = 28) {
  return {
    inverter_id: Number(inverterId),
    strings: Array.from({ length: count }, (_, idx) => ({
      string_index: idx + 1,
      enabled: true,
      has_data: false,
      current_a: null,
      last_ts: null
    }))
  };
}

// ======================================================
// SÉRIES REAIS (API)
// ======================================================
let DAILY = null;
let MONTHLY = null;
let ACTIVE_ALARMS = [];
let INVERTERS_REALTIME = [];
let RELAY_REALTIME = null; // ✅ NEW

const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
const PLANT_ID = new URLSearchParams(window.location.search).get("plant_id") || "1";

function normalizeApiBody(data) {
  if (data && data.body) {
    return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
  }
  return data;
}

function getUserContext() {
  try {
    const user = JSON.parse(localStorage.getItem("user"));
    return {
      customer_id: user?.customer_id ?? null,
      is_superuser: user?.is_superuser ?? false
    };
  } catch {
    return { customer_id: null, is_superuser: false };
  }
}

function buildAuthHeaders() {
  const ctx = getUserContext();
  const headers = { "Content-Type": "application/json" };
  if (ctx.customer_id) headers["X-Customer-Id"] = ctx.customer_id;
  if (ctx.is_superuser) headers["X-Is-Superuser"] = "true";
  return headers;
}

// ======================================================
// ✅ PREFERÊNCIAS — STRINGS DESABILITADAS (LOCALSTORAGE)
// ======================================================
function getPrefKey() {
  const ctx = getUserContext();
  const customer = ctx.customer_id ?? "anon";
  return `scada:strings_disabled:${customer}:${PLANT_ID}`;
}

function readDisabledPrefs() {
  try {
    return JSON.parse(localStorage.getItem(getPrefKey())) || {};
  } catch {
    return {};
  }
}

function isDisabledPref(inverterRealId, stringIndex) {
  const prefs = readDisabledPrefs();
  return !!prefs?.[String(inverterRealId)]?.[String(stringIndex)];
}

function setDisabledPref(inverterRealId, stringIndex, disabled) {
  const prefs = readDisabledPrefs();
  const invKey = String(inverterRealId);
  const sKey = String(stringIndex);

  prefs[invKey] = prefs[invKey] || {};
  if (disabled) prefs[invKey][sKey] = true;
  else delete prefs[invKey][sKey];

  localStorage.setItem(getPrefKey(), JSON.stringify(prefs));
}

// ======================================================
// FETCH — TEMPO REAL, WEATHER, ALARMES, ENERGIA
// ======================================================
async function fetchPlantRealtime(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/realtime`, {
    headers: buildAuthHeaders()
  });
  const data = await res.json();
  return normalizeApiBody(data);
}

async function fetchActiveAlarms(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/alarms/active`, {
    headers: buildAuthHeaders()
  });
  const data = await res.json();
  return normalizeApiBody(data);
}

async function fetchDailyEnergy(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/energy/daily`, {
    headers: buildAuthHeaders()
  });
  const data = await res.json();
  return normalizeApiBody(data);
}

async function fetchMonthlyEnergy(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/energy/monthly`, {
    headers: buildAuthHeaders()
  });
  const data = await res.json();
  return normalizeApiBody(data);
}

// ✅ NEW: relay realtime
async function fetchRelayRealtime(plantId) {
  const url = `${API_BASE}/plants/${plantId}/relay/realtime`;
  const res = await fetch(url, { headers: buildAuthHeaders() });
  if (!res.ok) {
    console.warn(`[relay/realtime] HTTP ${res.status} em ${url}`);
    return { item: null };
  }
  return normalizeApiBody(await res.json());
}

// ✅ realtime por inversor
async function fetchInvertersRealtime(plantId) {
  const candidates = [
    `${API_BASE}/plants/${plantId}/inverters/realtime`,
    `${API_BASE}/plants/${plantId}/inverters`
  ];

  for (const url of candidates) {
    const res = await fetch(url, { headers: buildAuthHeaders() });
    if (res.ok) {
      const data = normalizeApiBody(await res.json());
      return Array.isArray(data) ? data : (data?.items || []);
    }

    if (res.status === 404) continue;
    console.warn(`[inverters realtime] HTTP ${res.status} em ${url}`);
  }

  console.warn("[inverters realtime] nenhum endpoint disponível -> mantendo estático");
  return [];
}

// config (enabled/has_data)
async function fetchInverterStrings(plantId, inverterRealId) {
  const url = `${API_BASE}/plants/${plantId}/inverters/${inverterRealId}/strings`;
  const res = await fetch(url, { headers: buildAuthHeaders() });

  if (!res.ok) {
    console.warn(`[strings] ${res.status} em ${url}`);
    return null;
  }
  return normalizeApiBody(await res.json());
}

// medida (current_a)
async function fetchInverterStringsRealtime(plantId, inverterRealId) {
  const url = `${API_BASE}/plants/${plantId}/inverters/${inverterRealId}/strings/realtime`;
  const res = await fetch(url, { headers: buildAuthHeaders() });

  if (!res.ok) {
    console.warn(`[strings/realtime] ${res.status} em ${url}`);
    return null;
  }
  return normalizeApiBody(await res.json());
}

async function patchInverterString(plantId, inverterRealId, stringIndex, enabled) {
  const url = `${API_BASE}/plants/${plantId}/inverters/${inverterRealId}/strings/${stringIndex}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: buildAuthHeaders(),
    body: JSON.stringify({ enabled })
  });

  if (!res.ok) {
    throw new Error(`PATCH string falhou: HTTP ${res.status}`);
  }
  return normalizeApiBody(await res.json());
}

// ======================================================
// ✅ RESOLVER UI inverter (1..8) -> inverter_id REAL (device_id)
//   FIX PRINCIPAL: aceitar nomes "Inversor1", "Inverter1", "INVERSOR_1", etc.
// ======================================================
function getUiInverterNumberFromName(name) {
  if (!name) return null;
  const s = String(name).trim();

  // aceita:
  // - "Inversor 1" / "Inversor1" / "INVERSOR_1" / "inverter-1" / "Inverter 01"
  const m = s.match(/(inversor|inverter)\s*[_-]?\s*0*([1-8])$/i);
  if (m) return Number(m[2]);

  // fallback: se vier apenas "1"..."8"
  const n = s.match(/^0*([1-8])$/);
  if (n) return Number(n[1]);

  return null;
}

function getInvTsMs(inv) {
  const iso =
    inv.last_reading_at ??
    inv.last_reading_ts ??
    inv.last_ts ??
    inv.timestamp ??
    inv.event_ts ??
    null;

  const ms = iso ? new Date(iso).getTime() : NaN;
  return Number.isFinite(ms) ? ms : 0;
}

function buildInvertersIndex(inverters) {
  const map = new Map();

  (Array.isArray(inverters) ? inverters : []).forEach(inv => {
    // ✅ pega “nome” de vários campos possíveis
    const name =
      inv.inverter_name ??
      inv.device_name ??
      inv.deviceName ??
      inv.name ??
      inv.device_type ??
      inv.deviceType ??
      null;

    const uiId = getUiInverterNumberFromName(name);
    if (!uiId) return;

    const key = String(uiId);
    const prev = map.get(key);

    // fica com o mais recente
    if (!prev || getInvTsMs(inv) > getInvTsMs(prev)) {
      map.set(key, inv);
    }
  });

  return map;
}

function resolveInverterRealIdFromUi(uiId) {
  const idx = buildInvertersIndex(INVERTERS_REALTIME);
  const inv = idx.get(String(uiId));
  if (!inv) return null;

  return (
    inv.inverter_id ??
    inv.device_id ??
    inv.deviceId ??
    inv.id ??
    null
  );
}

// ======================================================
// RENDER — HEADER DA USINA
// ======================================================
function renderHeaderSummary() {
  const elRated = document.getElementById("headerRatedPower");
  const elActive = document.getElementById("headerActivePower");
  const elCapacity = document.getElementById("headerCapacity");
  if (!elRated || !elActive || !elCapacity) return;

  elRated.textContent = `${asNumber(PLANT_STATE.rated_power_kwp).toFixed(1)} kWp`;
  elActive.textContent = `${asNumber(PLANT_STATE.active_power_kw).toFixed(1)} kW`;
  elCapacity.textContent = `${asNumber(PLANT_STATE.capacity_percent).toFixed(1)} %`;
}

// ======================================================
// RENDER — WEATHER
// ======================================================
function renderWeather(data) {
  if (!data) return;

  const elIrr = document.getElementById("weatherIrradiance");
  const elAir = document.getElementById("weatherAirTemp");
  const elModule = document.getElementById("weatherModuleTemp");

  if (elIrr) {
    const value = data.irradiance_poa_wm2 ?? data.irradiance_ghi_wm2;
    elIrr.textContent = value != null ? `${Number(value).toFixed(0)} W/m²` : "—";
  }

  if (elAir) {
    elAir.textContent =
      data.air_temperature_c != null
        ? `${Number(data.air_temperature_c).toFixed(1)} °C`
        : "—";
  }

  if (elModule) {
    elModule.textContent =
      data.module_temperature_c != null
        ? `${Number(data.module_temperature_c).toFixed(1)} °C`
        : "—";
  }
}

// ======================================================
// RENDER — ALARMES ATIVOS
// ======================================================
function renderAlarms(alarms) {
  const container = document.getElementById("plantActiveAlarms");
  if (!container) return;

  container.innerHTML = "";

  if (!alarms || !alarms.length) {
    container.textContent = "Nenhum alarme ativo";
    return;
  }

  alarms.forEach(a => {
    const row = document.createElement("div");
    row.className = `alarm-row ${a.severity || ""}`.trim();

    row.innerHTML = `
      <span>${a.device_type || "—"} • ${a.device_name || "—"}</span>
      <span>${a.event_name || (a.event_code != null ? `Evento ${a.event_code}` : "—")}</span>
      <span>${a.started_at ? new Date(a.started_at).toLocaleString() : "—"}</span>
    `;

    container.appendChild(row);
  });
}

// ======================================================
// ✅ RENDER — RELÉ (NOVO SHAPE DO ENDPOINT /relay/realtime)
// item: { is_online, relay_on, last_update, analog:{active_power_kw} }
// ======================================================
function ensureRelayUiScaffold() {
  const relayRow = document.getElementById("relayRow");
  if (!relayRow) return null;

  const nameEl = relayRow.querySelector(".device-name");
  const dotEl = document.getElementById("relayDot") || relayRow.querySelector(".status-dot");

  // Remove “extras antigos” visualmente (não remove do DOM, só não usa)
  const oldOnline = document.getElementById("relayOnlineText");
  const oldAvail = document.getElementById("relayAvailabilityText");
  const oldLast = document.getElementById("relayLastUpdateText");

  if (oldOnline) oldOnline.textContent = "—";
  if (oldAvail) oldAvail.textContent = "";
  if (oldLast) oldLast.textContent = "";

  // cria badge ONLINE/OFFLINE ao lado do nome
  let badgeOnline = relayRow.querySelector("#relayOnlineBadge");
  if (!badgeOnline) {
    badgeOnline = document.createElement("span");
    badgeOnline.id = "relayOnlineBadge";
    badgeOnline.style.display = "inline-flex";
    badgeOnline.style.alignItems = "center";
    badgeOnline.style.justifyContent = "center";
    badgeOnline.style.padding = "6px 10px";
    badgeOnline.style.borderRadius = "999px";
    badgeOnline.style.fontSize = "11px";
    badgeOnline.style.letterSpacing = "0.06em";
    badgeOnline.style.textTransform = "uppercase";
    badgeOnline.style.border = "1px solid rgba(255,255,255,0.10)";
    badgeOnline.style.background = "rgba(255,255,255,0.04)";
    badgeOnline.style.color = "rgba(233,255,243,0.88)";
    badgeOnline.style.marginLeft = "10px";
    badgeOnline.style.whiteSpace = "nowrap";

    if (nameEl) nameEl.appendChild(badgeOnline);
  }

  // cria badge ON/OFF do relé
  let badgeState = relayRow.querySelector("#relayStateBadge");
  if (!badgeState) {
    badgeState = document.createElement("span");
    badgeState.id = "relayStateBadge";
    badgeState.style.display = "inline-flex";
    badgeState.style.alignItems = "center";
    badgeState.style.justifyContent = "center";
    badgeState.style.padding = "6px 10px";
    badgeState.style.borderRadius = "999px";
    badgeState.style.fontSize = "11px";
    badgeState.style.letterSpacing = "0.06em";
    badgeState.style.textTransform = "uppercase";
    badgeState.style.border = "1px solid rgba(255,255,255,0.10)";
    badgeState.style.background = "rgba(255,255,255,0.04)";
    badgeState.style.color = "rgba(233,255,243,0.88)";
    badgeState.style.marginLeft = "10px";
    badgeState.style.whiteSpace = "nowrap";

    if (nameEl) nameEl.appendChild(badgeState);
  }

  // cria o kW na direita (no lugar “—” que você quer)
  let powerEl = relayRow.querySelector("#relayPowerText");
  if (!powerEl) {
    powerEl = document.createElement("span");
    powerEl.id = "relayPowerText";
    powerEl.style.justifySelf = "end";
    powerEl.style.textAlign = "right";
    powerEl.style.whiteSpace = "nowrap";
    powerEl.style.fontWeight = "700";
    powerEl.style.color = "rgba(233,255,243,0.92)";
    powerEl.style.opacity = "0.95";
    powerEl.style.textShadow = "0 0 12px rgba(57,229,140,0.10)";

    // garante grid com 3 colunas (dot | nome | direita)
    relayRow.style.gridTemplateColumns = "14px 1fr auto";
    relayRow.appendChild(powerEl);
  }

  // cria o timestamp discretinho abaixo do nome (opcional)
  let tsEl = relayRow.querySelector("#relayTsText");
  if (!tsEl) {
    tsEl = document.createElement("div");
    tsEl.id = "relayTsText";
    tsEl.style.marginTop = "4px";
    tsEl.style.fontSize = "12px";
    tsEl.style.opacity = "0.75";
    tsEl.style.color = "rgba(154,219,184,0.85)";

    // coloca dentro do device-name (abaixo do texto)
    if (nameEl) nameEl.appendChild(tsEl);
  }

  return { relayRow, nameEl, dotEl, badgeOnline, badgeState, powerEl, tsEl };
}

function renderRelayCard(relayItem) {
  const ui = ensureRelayUiScaffold();
  if (!ui) return;

  const { relayRow, badgeOnline, badgeState, powerEl, tsEl } = ui;

  // sem dados ainda
  if (!relayItem) {
    relayRow.classList.remove("online", "offline");
    badgeOnline.textContent = "—";
    badgeState.textContent = "—";
    powerEl.textContent = "— kW";
    tsEl.textContent = "Última atualização: —";
    return;
  }

  const isOnline = relayItem.is_online === true;
  const relayOn = relayItem.relay_on; // true/false/null
  const lastUpdate = relayItem.last_update ?? null;

  const kw = relayItem?.analog?.active_power_kw;
  const kwText = (kw === null || kw === undefined || Number.isNaN(Number(kw)))
    ? "— kW"
    : `${numFixedOrDash(kw, 1)} kW`;

  // classes do row (para a bolinha)
  relayRow.classList.remove("online", "offline");
  relayRow.classList.add(isOnline ? "online" : "offline");

  // badge online/offline
  badgeOnline.textContent = isOnline ? "ONLINE" : "OFFLINE";
  badgeOnline.style.borderColor = isOnline ? "rgba(57,229,140,0.26)" : "rgba(255,92,92,0.25)";
  badgeOnline.style.background = isOnline ? "rgba(57,229,140,0.08)" : "rgba(255,92,92,0.08)";
  badgeOnline.style.color = isOnline ? "rgba(233,255,243,0.92)" : "rgba(255,255,255,0.92)";

  // badge ON/OFF
  let stateText = "—";
  if (relayOn === true) stateText = "ON";
  else if (relayOn === false) stateText = "OFF";

  badgeState.textContent = stateText;

  if (stateText === "ON") {
    badgeState.style.borderColor = "rgba(57,229,140,0.30)";
    badgeState.style.background = "rgba(57,229,140,0.10)";
    badgeState.style.color = "rgba(233,255,243,0.95)";
    badgeState.style.boxShadow = "0 0 18px rgba(57,229,140,0.12)";
  } else if (stateText === "OFF") {
    badgeState.style.borderColor = "rgba(255,92,92,0.28)";
    badgeState.style.background = "rgba(255,92,92,0.08)";
    badgeState.style.color = "rgba(255,255,255,0.95)";
    badgeState.style.boxShadow = "0 0 16px rgba(255,92,92,0.10)";
  } else {
    badgeState.style.borderColor = "rgba(255,255,255,0.10)";
    badgeState.style.background = "rgba(255,255,255,0.04)";
    badgeState.style.color = "rgba(233,255,243,0.88)";
    badgeState.style.boxShadow = "none";
  }

  // kW à direita
  powerEl.textContent = kwText;

  // timestamp
  tsEl.textContent = `Última atualização: ${fmtDatePtBR(lastUpdate)}`;
}

// ======================================================
// ✅ RENDER — INVERTERS (KPIs por inversor) ✅
// ======================================================
function fillInverterRowSpans(rowEl, values) {
  const spans = rowEl.querySelectorAll(":scope > span");
  if (!spans || spans.length < 8) return false;

  spans[2].textContent = values.power;
  spans[3].textContent = values.eff;
  spans[4].textContent = values.temp;
  spans[5].textContent = values.freq;
  spans[6].textContent = values.pr;
  spans[7].textContent = values.last;
  return true;
}

function setRowOnlineUi(rowEl, online) {
  rowEl.classList.remove("online", "offline");
  rowEl.classList.add(online ? "online" : "offline");

  const dot = rowEl.querySelector(".status-dot, [data-role='status-dot']");
  if (dot) {
    dot.classList.remove("online", "offline");
    dot.classList.add(online ? "online" : "offline");
  }
}

function isOnlineByFreshness(inv) {
  const lastMs = getInvTsMs(inv);
  if (!lastMs) return false;
  const ageMs = Date.now() - lastMs;
  return ageMs <= INVERTER_ONLINE_AFTER_MS;
}

function renderInverterRowKpis(rowEl, inv) {
  const powerKw = inv.active_power_kw ?? inv.power_kw ?? inv.power ?? inv.active_power;
  const effPct  = inv.efficiency_pct ?? inv.efficiency ?? inv.eff_pct;
  const tempC   = inv.temperature_internal_c ?? inv.temperature_c ?? inv.temp_c ?? inv.temperature_current ?? inv.temperature;
  const freqHz  = inv.frequency_hz ?? inv.freq_hz ?? inv.frequency;

  const prRaw = inv.performance_ratio ?? inv.pr ?? inv.pr_ratio ?? inv.performance;
  const lastTs =
    inv.last_reading_at ??
    inv.last_reading_ts ??
    inv.last_ts ??
    inv.timestamp ??
    inv.event_ts ??
    null;

  const prPct = normalizePercentMaybe(prRaw);

  const powerText = powerKw != null ? `${numFixedOrDash(powerKw, 0)} kW` : "—";
  const effText   = effPct  != null ? `${numFixedOrDash(effPct, 1)} %` : "—";
  const tempText  = tempC   != null ? `${numFixedOrDash(tempC, 1)} °C` : "—";
  const freqText  = freqHz  != null ? `${numFixedOrDash(freqHz, 2)} Hz` : "—";
  const prText    = prPct   != null ? `${numFixedOrDash(prPct, 2)} %` : "—";
  const lastText  = fmtDatePtBR(lastTs);

  fillInverterRowSpans(rowEl, {
    power: powerText,
    eff: effText,
    temp: tempText,
    freq: freqText,
    pr: prText,
    last: lastText
  });

  const online = isOnlineByFreshness(inv);
  setRowOnlineUi(rowEl, online);
}

function renderInvertersRows(inverters) {
  const idx = buildInvertersIndex(inverters);

  const rows = document.querySelectorAll(".inverter-toggle[data-inverter]");
  if (!rows || !rows.length) return;

  rows.forEach(row => {
    const uiId = row.dataset.inverter;
    const inv = idx.get(String(uiId));

    if (!inv) {
      fillInverterRowSpans(row, {
        power: "—",
        eff: "—",
        temp: "—",
        freq: "—",
        pr: "—",
        last: "—"
      });
      setRowOnlineUi(row, false);
      return;
    }

    renderInverterRowKpis(row, inv);
  });
}

// ======================================================
// ✅ MERGE: config(/strings) + realtime(/strings/realtime)
// ======================================================
function mergeStringsPayload(configPayload, realtimePayload, inverterRealId) {
  const maxStrings = 30;

  const cfgList = configPayload?.strings ?? [];
  const rtList = realtimePayload?.items ?? realtimePayload?.strings ?? [];

  const cfgMap = new Map(cfgList.map(s => [Number(s.string_index), s]));
  const rtMap = new Map(rtList.map(s => [Number(s.string_index), s]));

  const strings = [];
  for (let i = 1; i <= maxStrings; i++) {
    const cfg = cfgMap.get(i);
    const rt = rtMap.get(i);

    const enabled = cfg ? !!cfg.enabled : true;
    const has_data = (rt?.has_data ?? cfg?.has_data ?? false) === true;

    strings.push({
      string_index: i,
      enabled,
      has_data,
      current_a: rt?.current_a ?? null,
      last_ts: rt?.last_ts ?? null
    });
  }

  return {
    inverter_id: Number(inverterRealId),
    max_strings: maxStrings,
    strings
  };
}

// ======================================================
// RENDER — STRINGS (COM PERSISTÊNCIA LOCAL + VALOR REAL)
// ======================================================
function renderStringsGrid(gridEl, payload) {
  if (!gridEl) return;

  const strings = payload?.strings ?? [];
  gridEl.innerHTML = "";

  if (!strings.length) {
    gridEl.innerHTML = `<div style="color:#9adbb8; opacity:.7; padding:6px 2px;">Sem dados de strings</div>`;
    return;
  }

  const inverterRealId = payload.inverter_id;

  strings.forEach(s => {
    const el = document.createElement("div");
    el.className = "string-card";
    el.dataset.string = s.string_index;

    const disabledByPref = isDisabledPref(inverterRealId, s.string_index);
    const effectiveEnabled = disabledByPref ? false : !!s.enabled;

    if (!effectiveEnabled) el.classList.add("disabled");
    else if (!s.has_data) el.classList.add("nodata");
    else el.classList.add("active");

    el.dataset.inverterId = inverterRealId;
    el.dataset.stringIndex = s.string_index;
    el.dataset.enabled = String(effectiveEnabled);

    const ampText = s.has_data ? fmtAmp(s.current_a) : "—";

    el.innerHTML = `
      S${s.string_index}
      <strong>${ampText}</strong>
    `;

    el.addEventListener("click", async (e) => {
      e.stopPropagation();

      const currentEnabled = el.dataset.enabled === "true";
      const nextEnabled = !currentEnabled;

      setDisabledPref(inverterRealId, s.string_index, !nextEnabled);

      el.dataset.enabled = String(nextEnabled);
      if (nextEnabled) {
        el.classList.remove("disabled");
        el.classList.toggle("nodata", !s.has_data);
        el.classList.toggle("active", !!s.has_data);
      } else {
        el.classList.add("disabled");
        el.classList.remove("nodata", "active");
      }

      try {
        await patchInverterString(PLANT_ID, inverterRealId, s.string_index, nextEnabled);
      } catch (error) {
        console.warn("PATCH falhou (mantendo preferência local):", error?.message || error);
      }
    });

    gridEl.appendChild(el);
  });
}

// ======================================================
// RENDER — FAIXA OPERACIONAL (se existir no HTML)
// ======================================================
function renderSummaryStrip() {
  const elActive = document.getElementById("summaryActivePower");
  const elRated = document.getElementById("summaryRatedPower");
  const elInv = document.getElementById("summaryInverters");
  const elPR = document.getElementById("summaryPR");

  if (!elActive || !elRated || !elInv || !elPR) return;

  elActive.textContent = `${asNumber(PLANT_STATE.active_power_kw).toFixed(1)} kW`;
  elRated.textContent = `${asNumber(PLANT_STATE.rated_power_kwp).toFixed(1)} kWp`;
  elInv.textContent = `${PLANT_STATE.inverter_online} / ${PLANT_STATE.inverter_total} Online`;
  elPR.textContent = `${asNumber(PLANT_STATE.pr_percent).toFixed(1)} %`;
}

// ======================================================
// CHART INSTANCES
// ======================================================
let dailyChartInstance = null;
let monthlyChartInstance = null;

// ======================================================
// GRÁFICO DIÁRIO
// ======================================================
function renderDailyChart() {
  const canvas = document.getElementById("plantMainChart");
  if (!canvas || !DAILY?.labels?.length) return;

  const ctx = canvas.getContext("2d");

  if (dailyChartInstance) {
    dailyChartInstance.destroy();
    dailyChartInstance = null;
  }

  const greenGradient = ctx.createLinearGradient(0, 0, 0, 320);
  greenGradient.addColorStop(0, "rgba(57,229,140,0.55)");
  greenGradient.addColorStop(0.6, "rgba(57,229,140,0.35)");
  greenGradient.addColorStop(1, "rgba(57,229,140,0.05)");

  const yellowGradient = ctx.createLinearGradient(0, 0, 0, 320);
  yellowGradient.addColorStop(0, "rgba(255,216,77,0.45)");
  yellowGradient.addColorStop(1, "rgba(255,216,77,0.05)");

  dailyChartInstance = new Chart(ctx, {
    type: "line",
    data: {
      labels: DAILY.labels,
      datasets: [
        {
          data: DAILY.activePower,
          borderColor: "#39e58c",
          backgroundColor: greenGradient,
          fill: true,
          tension: 0.4,
          pointRadius: 0,
          borderWidth: 2,
          yAxisID: "yPower",
          spanGaps: true
        },
        {
          data: DAILY.irradiance,
          borderColor: "#ffd84d",
          backgroundColor: yellowGradient,
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 2,
          yAxisID: "yIrr",
          spanGaps: true
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { display: false } },
      scales: {
        x: {
          ticks: { color: "#9adbb8", maxTicksLimit: 12 },
          grid: { color: "rgba(255,255,255,0.04)" }
        },
        yPower: {
          position: "left",
          min: 0,
          max: 2000,
          ticks: { color: "#39e58c", callback: v => `${v} kW` },
          grid: { color: "rgba(255,255,255,0.05)" }
        },
        yIrr: {
          position: "right",
          min: 0,
          max: 1200,
          ticks: { color: "#ffd84d", callback: v => `${v} W/m²` },
          grid: { drawOnChartArea: false }
        }
      }
    }
  });
}

// ======================================================
// ✅ NORMALIZA PAYLOAD MENSAL (unidade + outlier + labels)
// ======================================================
function normalizeMonthlyPayload(payload) {
  if (!payload) return payload;

  const labels = Array.isArray(payload.labels) ? payload.labels.slice() : [];

  const dailyNew = Array.isArray(payload.daily_kwh) ? payload.daily_kwh.slice() : null;
  const mtdNew = Array.isArray(payload.mtd_kwh) ? payload.mtd_kwh.slice() : null;
  const energyLegacy = Array.isArray(payload.energy_kwh) ? payload.energy_kwh.slice() : null;

  let daily = (dailyNew ?? energyLegacy ?? []).map(v => Number(v) || 0);

  const n = Math.min(labels.length || daily.length, daily.length || labels.length);
  const cutLabels = labels.slice(0, n);
  daily = daily.slice(0, n);

  const allLookDayOnly = cutLabels.length > 0 && cutLabels.every(looksLikeDayOnlyLabel);
  const duplicated = hasDuplicateLabels(cutLabels);

  const finalLabels =
    allLookDayOnly && duplicated ? buildLastNDaysLabels(daily.length) : cutLabels;

  let mtd = [];
  if (mtdNew && mtdNew.length >= daily.length) {
    mtd = mtdNew.slice(0, daily.length).map(v => Number(v) || 0);
  } else {
    let acc = 0;
    mtd = daily.map(v => (acc += (Number(v) || 0)));
  }

  const converted = maybeConvertWhToKwh(daily, mtd);
  daily = converted.daily;
  mtd = converted.mtd;

  const capped = capMonthlyOutliers(daily);
  daily = capped.daily;

  let acc = 0;
  mtd = daily.map(v => (acc += (Number(v) || 0)));

  return {
    ...payload,
    labels: finalLabels,
    daily_kwh: daily,
    mtd_kwh: mtd,
    energy_kwh: daily
  };
}

// ======================================================
// GRÁFICO MENSAL — SÓ BARRAS (SEM LINHA)
// ======================================================
function renderMonthlyChart() {
  const canvas = document.getElementById("plantMonthlyChart");
  if (!canvas) return;

  const labels = Array.isArray(MONTHLY?.labels) ? MONTHLY.labels : [];
  const daily = Array.isArray(MONTHLY?.daily_kwh)
    ? MONTHLY.daily_kwh.map(v => Number(v) || 0)
    : (Array.isArray(MONTHLY?.energy_kwh) ? MONTHLY.energy_kwh.map(v => Number(v) || 0) : []);

  if (!labels.length || !daily.length) return;

  const ctx = canvas.getContext("2d");

  if (monthlyChartInstance) {
    monthlyChartInstance.destroy();
    monthlyChartInstance = null;
  }

  const maxDaily = Math.max(...daily, 0);
  const suggestedMaxDaily = maxDaily > 0 ? Math.ceil(maxDaily * 1.25) : undefined;

  monthlyChartInstance = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          data: daily,
          backgroundColor: "rgba(200,200,200,0.75)",
          borderRadius: 8,
          barThickness: 18,
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => items?.[0]?.label ? `Dia ${items[0].label}` : "",
            label: (item) => `Geração do dia: ${formatKwhPtBR(item?.raw)}`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#9adbb8", maxTicksLimit: 8 },
          grid: { display: false }
        },
        y: {
          beginAtZero: true,
          suggestedMax: suggestedMaxDaily,
          ticks: { color: "#9adbb8", callback: (v) => formatNumberPtBR(v) },
          grid: { color: "rgba(255,255,255,0.04)" }
        }
      }
    }
  });
}

// ======================================================
// TOGGLE — abre/fecha o painel
// ======================================================
function setupInverterToggles() {
  const container = document.querySelector(".inverters-section");
  if (!container) return;

  container.addEventListener("click", async (e) => {
    if (e.target.closest(".string-card")) return;

    const row = e.target.closest(".inverter-toggle");
    if (!row) return;

    const uiId = row.dataset.inverter; // "1".."8"
    const panel = document.getElementById(`strings-${uiId}`);
    if (!panel) return;

    const willOpen = !row.classList.contains("open");

    document.querySelectorAll(".inverter-row.open, .inverter-toggle.open").forEach(r => r.classList.remove("open"));
    document.querySelectorAll(".inverter-strings.open").forEach(p => {
      p.classList.remove("open");
      p.style.maxHeight = "0px";
      p.style.opacity = "0";
    });

    if (!willOpen) return;

    row.classList.add("open");
    panel.classList.add("open");

    panel.style.opacity = "1";
    panel.style.maxHeight = panel.scrollHeight + "px";

    await refreshStringsForUiInverter(uiId);
    panel.style.maxHeight = panel.scrollHeight + "px";
  });
}

// ======================================================
// ✅ Atualiza strings de um inversor UI (1..8) chamando API
// ======================================================
async function refreshStringsForUiInverter(uiId) {
  const grid = document.querySelector(`.strings-grid[data-inverter-id="${uiId}"]`);
  if (!grid) return;

  const inverterRealId = resolveInverterRealIdFromUi(uiId);

  if (!inverterRealId) {
    const mock = buildMockStringsPayload(uiId, 28);
    renderStringsGrid(grid, mock);
    return;
  }

  const [cfg, rt] = await Promise.all([
    fetchInverterStrings(PLANT_ID, inverterRealId),
    fetchInverterStringsRealtime(PLANT_ID, inverterRealId)
  ]);

  const merged = mergeStringsPayload(cfg, rt, inverterRealId);
  renderStringsGrid(grid, merged);
}

// ======================================================
// ✅ Atualiza apenas o inversor que estiver ABERTO
// ======================================================
async function refreshOpenStringsPanels() {
  const openRow = document.querySelector(".inverter-toggle.open[data-inverter]");
  if (!openRow) return;
  const uiId = openRow.dataset.inverter;
  await refreshStringsForUiInverter(uiId);
}

// ======================================================
// ✅ REFRESH (realtime + alarms + inverters rows + strings abertas + relay)
// ======================================================
async function refreshRealtimeEverything() {
  try {
    const realtime = await fetchPlantRealtime(PLANT_ID);

    if (realtime) {
      const rated = asNumber(realtime.rated_power_kw, PLANT_STATE.rated_power_kwp);
      const active = asNumber(realtime.active_power_kw, PLANT_STATE.active_power_kw);

      const avail = realtime.inverter_availability_pct; // esperado 0..1
      const invOnline =
        Number.isFinite(Number(avail))
          ? Math.round(Number(avail) * Number(PLANT_STATE.inverter_total || 0))
          : PLANT_STATE.inverter_online;

      const prPct = normalizePercentMaybe(realtime.performance_ratio);

      PLANT_STATE = {
        ...PLANT_STATE,
        rated_power_kwp: rated,
        active_power_kw: active,
        capacity_percent: rated > 0 ? (active / rated) * 100 : PLANT_STATE.capacity_percent,
        inverter_online: invOnline,
        pr_percent: prPct != null ? prPct : PLANT_STATE.pr_percent
      };
    }

    renderHeaderSummary();
    if (realtime?.weather) renderWeather(realtime.weather);
    renderSummaryStrip();

    ACTIVE_ALARMS = await fetchActiveAlarms(PLANT_ID);
    renderAlarms(ACTIVE_ALARMS);

    INVERTERS_REALTIME = await fetchInvertersRealtime(PLANT_ID);
    renderInvertersRows(INVERTERS_REALTIME);

    // ✅ RELAY realtime (novo shape)
    const relayPayload = await fetchRelayRealtime(PLANT_ID);
    RELAY_REALTIME = relayPayload?.item ?? null;
    renderRelayCard(RELAY_REALTIME);

    await refreshOpenStringsPanels();
  } catch (e) {
    console.error("[refreshRealtimeEverything] erro", e);
    renderHeaderSummary();
    renderSummaryStrip();
  }
}

// ======================================================
// INIT
// ======================================================
document.addEventListener("DOMContentLoaded", async () => {
  setupInverterToggles();

  try {
    await refreshRealtimeEverything();

    const grids = document.querySelectorAll(".strings-grid[data-inverter-id]");
    for (const grid of grids) {
      const uiId = grid.dataset.inverterId; // "1".."8"
      await refreshStringsForUiInverter(uiId);

      const panel = document.getElementById(`strings-${uiId}`);
      if (panel?.classList.contains("open")) {
        panel.style.maxHeight = panel.scrollHeight + "px";
      }
    }

    // ✅ DAILY: normaliza para sempre começar em 00:00
    const daily = await fetchDailyEnergy(PLANT_ID);
    if (daily?.labels?.length) {
      DAILY = normalizeDailyPayload(daily);
      renderDailyChart();
    }

    const monthlyRaw = await fetchMonthlyEnergy(PLANT_ID);
    if (monthlyRaw) {
      MONTHLY = normalizeMonthlyPayload(monthlyRaw);
      renderMonthlyChart();
    }

    setInterval(refreshRealtimeEverything, 30000);
  } catch (e) {
    console.error(e);
    renderHeaderSummary();
    renderSummaryStrip();
  }
});
