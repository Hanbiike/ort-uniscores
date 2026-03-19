const DATA_URL = "./data/dataset.json";

const SCORE_LABELS = {
  primary: "основной",
  additional: "дополнительный",
  total: "общий",
};

const METRIC_LABELS = {
  lower_passing_score: "нижний порог",
  average_score: "средний балл",
  median_score: "медианный балл",
};

const state = {
  dataset: null,
  scope: "universities",
  scoreType: "primary",
  metric: "lower_passing_score",
  query: "",
  limit: 100,
  minRecords: 5,
};

const refs = {
  scopeSelect: document.getElementById("scopeSelect"),
  scoreTypeSelect: document.getElementById("scoreTypeSelect"),
  metricSelect: document.getElementById("metricSelect"),
  ratingSearch: document.getElementById("ratingSearch"),
  limitSelect: document.getElementById("limitSelect"),
  minRecordsInput: document.getElementById("minRecordsInput"),
  ratingsNote: document.getElementById("ratingsNote"),
  ratingsSummary: document.getElementById("ratingsSummary"),
  ratingsTableBody: document.getElementById("ratingsTableBody"),
  ratingsEmpty: document.getElementById("ratingsEmpty"),
  entityHeader: document.getElementById("entityHeader"),
  universityHeader: document.getElementById("universityHeader"),
};

const REQUIRED_REF_KEYS = [
  "scopeSelect",
  "scoreTypeSelect",
  "metricSelect",
  "ratingSearch",
  "limitSelect",
  "minRecordsInput",
  "ratingsNote",
  "ratingsSummary",
  "ratingsTableBody",
  "ratingsEmpty",
  "entityHeader",
  "universityHeader",
];

document.addEventListener("DOMContentLoaded", () => {
  if (!validateDomRefs()) {
    return;
  }
  setupRevealAnimation();
  bindEvents();
  loadDataset();
});

function validateDomRefs() {
  const missing = REQUIRED_REF_KEYS.filter((key) => !refs[key]);
  if (!missing.length) {
    return true;
  }

  console.error("UNISCORES ratings: missing DOM refs", missing);
  return false;
}

function setupRevealAnimation() {
  const reveals = Array.from(document.querySelectorAll(".reveal"));
  reveals.forEach((node, index) => {
    window.setTimeout(() => {
      node.classList.add("ready");
    }, 90 * index);
  });
}

function applyEnterMotion(node, index = 0, step = 26, maxDelay = 280) {
  if (!node) {
    return;
  }
  const delay = Math.min(index * step, maxDelay);
  node.classList.add("animate-in");
  node.style.setProperty("--enter-delay", `${delay}ms`);
}

function bindEvents() {
  refs.scopeSelect.addEventListener("change", () => {
    state.scope = refs.scopeSelect.value;
    renderRatings();
  });

  refs.scoreTypeSelect.addEventListener("change", () => {
    state.scoreType = refs.scoreTypeSelect.value;
    renderRatings();
  });

  refs.metricSelect.addEventListener("change", () => {
    state.metric = refs.metricSelect.value;
    renderRatings();
  });

  refs.ratingSearch.addEventListener("input", () => {
    state.query = refs.ratingSearch.value.trim().toLowerCase();
    renderRatings();
  });

  refs.limitSelect.addEventListener("change", () => {
    state.limit = parseLimit(refs.limitSelect.value);
    renderRatings();
  });

  refs.minRecordsInput.addEventListener("input", () => {
    state.minRecords = parseMinRecords(refs.minRecordsInput.value);
    renderRatings();
  });

  refs.minRecordsInput.addEventListener("change", () => {
    state.minRecords = parseMinRecords(refs.minRecordsInput.value);
    refs.minRecordsInput.value = String(state.minRecords);
    renderRatings();
  });
}

async function loadDataset() {
  const candidates = [
    DATA_URL,
    "data/dataset.json",
    "/docs/data/dataset.json",
    "/data/dataset.json",
  ];

  try {
    const loaded = await fetchDatasetFromCandidates(candidates);
    state.dataset = loaded.dataset;

    refs.scopeSelect.value = state.scope;
    refs.scoreTypeSelect.value = state.scoreType;
    refs.metricSelect.value = state.metric;
    refs.limitSelect.value = String(state.limit);
    refs.minRecordsInput.value = String(state.minRecords);

    renderRatings();
  } catch (error) {
    refs.ratingsSummary.textContent =
      "Не удалось загрузить dataset.json. Выполните экспорт данных.";
    refs.ratingsTableBody.textContent = "";
    refs.ratingsEmpty.classList.remove("is-hidden");
    refs.ratingsNote.textContent = "";
    console.error("UNISCORES ratings: dataset load error", error);
  }
}

async function fetchDatasetFromCandidates(candidates) {
  const tried = [];

  for (const url of candidates) {
    try {
      const response = await fetch(url, { cache: "no-store" });
      tried.push(`${url} -> HTTP ${response.status}`);
      if (!response.ok) {
        continue;
      }
      const dataset = await response.json();
      return { url, dataset };
    } catch (error) {
      const message = String(error?.message || error);
      tried.push(`${url} -> ${message}`);
    }
  }

  throw new Error(`dataset.json не найден. Проверено: ${tried.join("; ")}`);
}

function renderRatings() {
  const rankings = state.dataset?.rankings;
  if (!rankings) {
    refs.ratingsSummary.textContent =
      "В dataset нет блока rankings. Перегенерируйте dataset новым exporter-скриптом.";
    refs.ratingsTableBody.textContent = "";
    refs.ratingsEmpty.classList.remove("is-hidden");
    refs.ratingsNote.textContent = "";
    return;
  }

  const source =
    state.scope === "universities" ? rankings.universities : rankings.directions;
  const entries = Array.isArray(source) ? source : [];

  const prepared = entries
    .map((entry) => prepareEntry(entry))
    .filter(Boolean)
    .filter((entry) => matchQuery(entry));

  prepared.sort(comparePreparedEntries);

  const limited = prepared.slice(0, state.limit);
  renderTable(limited);
  renderSummary(limited.length, prepared.length);
  renderNote(rankings.notes || null);
  updateHeaders();
}

function prepareEntry(entry) {
  const scoreStats = entry?.score_stats || {};
  if (state.scoreType !== "primary" && !scoreStats.has_additional) {
    return null;
  }

  const metricStats = scoreStats[state.scoreType];
  if (!metricStats) {
    return null;
  }

  const valuesCount = toNumber(metricStats.participants_count);
  if (!(valuesCount > 0)) {
    return null;
  }

  if (valuesCount < state.minRecords) {
    return null;
  }

  const metricValue = toNumber(metricStats[state.metric]);
  if (metricValue === null) {
    return null;
  }

  return {
    source: entry,
    stats: metricStats,
    metricValue,
    displayName: getDisplayName(entry),
    searchText: buildSearchText(entry),
  };
}

function matchQuery(prepared) {
  if (!state.query) {
    return true;
  }
  return prepared.searchText.includes(state.query);
}

function comparePreparedEntries(left, right) {
  if (right.metricValue !== left.metricValue) {
    return right.metricValue - left.metricValue;
  }

  const rightMedian = toNumber(right.stats.median_score) || -Infinity;
  const leftMedian = toNumber(left.stats.median_score) || -Infinity;
  if (rightMedian !== leftMedian) {
    return rightMedian - leftMedian;
  }

  return left.displayName.localeCompare(right.displayName, "ru");
}

function renderTable(rows) {
  refs.ratingsTableBody.textContent = "";

  if (!rows.length) {
    refs.ratingsEmpty.classList.remove("is-hidden");
    return;
  }

  refs.ratingsEmpty.classList.add("is-hidden");

  const fragment = document.createDocumentFragment();
  rows.forEach((row, index) => {
    const tr = document.createElement("tr");
    applyEnterMotion(tr, index, 18, 260);

    const rankCell = document.createElement("td");
    rankCell.className = "col-rank align-right";
    rankCell.textContent = String(index + 1);

    const entityCell = document.createElement("td");
    const entityTitle = document.createElement("strong");
    entityTitle.className = "rating-entity-title";
    entityTitle.textContent = row.displayName;

    entityCell.append(entityTitle);

    const entityMeta = buildEntityMeta(row.source);
    if (entityMeta) {
      const entityMetaNode = document.createElement("small");
      entityMetaNode.className = "rating-entity-meta";
      entityMetaNode.textContent = entityMeta;
      entityCell.append(entityMetaNode);
    }

    if (
      state.scope === "directions" &&
      state.scoreType === "additional" &&
      row.source?.requires_two_subjects_additional
    ) {
      const chip = document.createElement("span");
      chip.className = "rating-chip";
      chip.textContent = "доп/2";
      entityCell.append(chip);
    }

    const universityCell = document.createElement("td");
    universityCell.setAttribute("data-col", "university");
    universityCell.textContent =
      state.scope === "directions"
        ? row.source?.university_name || "-"
        : "-";

    const lowerCell = makeMetricCell(row.stats.lower_passing_score);
    const averageCell = makeMetricCell(row.stats.average_score);
    const medianCell = makeMetricCell(row.stats.median_score);
    const countCell = makeMetricCell(row.stats.participants_count, true);

    tr.append(
      rankCell,
      entityCell,
      universityCell,
      lowerCell,
      averageCell,
      medianCell,
      countCell
    );

    fragment.append(tr);
  });

  refs.ratingsTableBody.append(fragment);
}

function renderSummary(renderedCount, filteredCount) {
  const scopeLabel = state.scope === "universities" ? "вузов" : "направлений";
  const metricLabel = METRIC_LABELS[state.metric] || state.metric;
  refs.ratingsSummary.textContent =
    `Показано ${renderedCount} из ${filteredCount} ${scopeLabel}. ` +
    `Сортировка по метрике: ${metricLabel}. ` +
    `Минимум записей: ${state.minRecords}.`;
}

function renderNote(notes) {
  const scoreTypeLabel = SCORE_LABELS[state.scoreType] || state.scoreType;
  let text = `Тип балла: ${scoreTypeLabel}.`;

  if (state.scoreType === "additional") {
    const divisor = toNumber(notes?.additional_two_subject_divisor) || 2;
    text +=
      ` Для направлений с двумя профильными предметами используется ` +
      `дополнительный балл / ${divisor}.`;
  }

  refs.ratingsNote.textContent = text;
}

function updateHeaders() {
  const byDirections = state.scope === "directions";
  refs.entityHeader.textContent = byDirections ? "Направление" : "Вуз";
  refs.universityHeader.classList.toggle("is-hidden-column", !byDirections);

  const universityCells = refs.ratingsTableBody.querySelectorAll(
    'td[data-col="university"]'
  );
  universityCells.forEach((cell) => {
    cell.classList.toggle("is-hidden-column", !byDirections);
  });
}

function getDisplayName(entry) {
  if (state.scope === "universities") {
    return entry?.name || "Без названия";
  }

  const code = entry?.program_code ? `[${entry.program_code}] ` : "";
  const name = entry?.program_name || "Без названия";
  return `${code}${name}`;
}

function buildEntityMeta(entry) {
  if (state.scope === "universities") {
    return "";
  }

  return [entry?.faculty_name, entry?.payment_type]
    .filter(Boolean)
    .join(" | ");
}

function buildSearchText(entry) {
  const parts = state.scope === "universities"
    ? [entry?.name]
    : [
        entry?.program_name,
        entry?.program_code,
        entry?.faculty_name,
        entry?.payment_type,
        entry?.university_name,
      ];

  return parts
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function makeMetricCell(value, integerOnly = false) {
  const cell = document.createElement("td");
  cell.className = "align-right";
  if (integerOnly) {
    cell.textContent = formatInteger(value);
    return cell;
  }
  cell.textContent = formatScore(value);
  return cell;
}

function formatInteger(value) {
  const numeric = toNumber(value);
  if (numeric === null) {
    return "-";
  }
  return Math.round(numeric).toLocaleString("ru-RU");
}

function formatScore(value) {
  const numeric = toNumber(value);
  if (numeric === null) {
    return "-";
  }

  if (Number.isInteger(numeric)) {
    return numeric.toLocaleString("ru-RU");
  }

  return numeric.toLocaleString("ru-RU", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  if (Number.isNaN(number)) {
    return null;
  }

  return number;
}

function parseLimit(raw) {
  if (raw === "all") {
    return Number.POSITIVE_INFINITY;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100;
  }

  return parsed;
}

function parseMinRecords(raw) {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return 5;
  }

  const normalized = Math.floor(parsed);
  if (normalized < 1) {
    return 1;
  }

  return normalized;
}
