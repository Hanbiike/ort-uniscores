const DATA_URL = "./data/dataset.json";

const state = {
  dataset: null,
  selectedUniversityId: null,
  selectedProgramId: null,
};

const refs = {
  universityStep: document.getElementById("universityStep"),
  programStep: document.getElementById("programStep"),
  programStepCaption: document.getElementById("programStepCaption"),
  backToUniversities: document.getElementById("backToUniversities"),
  universitySearch: document.getElementById("universitySearch"),
  universityList: document.getElementById("universityList"),
  programSearch: document.getElementById("programSearch"),
  programList: document.getElementById("programList"),
  programMeta: document.getElementById("programMeta"),
  directionStatsOverall: document.getElementById("directionStatsOverall"),
  categoryStatsList: document.getElementById("categoryStatsList"),
};

const REQUIRED_REF_KEYS = [
  "universityStep",
  "programStep",
  "programStepCaption",
  "backToUniversities",
  "universitySearch",
  "universityList",
  "programSearch",
  "programList",
  "programMeta",
  "directionStatsOverall",
  "categoryStatsList",
];

const SCORE_KIND_CONFIG = [
  {
    key: "primary",
    title: "Основной балл",
    requiresAdditional: false,
  },
  {
    key: "additional",
    title: "Дополнительные баллы",
    requiresAdditional: true,
  },
  {
    key: "total",
    title: "Общий балл",
    requiresAdditional: true,
  },
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

  console.error("UNISCORES UI: missing DOM refs", missing);
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

function applyEnterMotion(node, index = 0, step = 36, maxDelay = 320) {
  if (!node) {
    return;
  }
  const delay = Math.min(index * step, maxDelay);
  node.classList.add("animate-in");
  node.style.setProperty("--enter-delay", `${delay}ms`);
}

function triggerStepEnter(node) {
  if (!node) {
    return;
  }
  node.classList.remove("step-enter");
  void node.offsetWidth;
  node.classList.add("step-enter");
}

function bindEvents() {
  refs.universitySearch.addEventListener("input", () => {
    renderUniversityList();
  });

  refs.programSearch.addEventListener("input", () => {
    renderProgramList();
  });

  refs.backToUniversities.addEventListener("click", () => {
    state.selectedUniversityId = null;
    state.selectedProgramId = null;
    refs.programSearch.value = "";
    renderAll();
    focusUniversityStep();
  });
}

async function loadDataset() {
  const candidates = [
    DATA_URL,
    "data/dataset.json",
    "/frontend-static/data/dataset.json",
    "/data/dataset.json",
  ];

  try {
    const loaded = await fetchDatasetFromCandidates(candidates);
    const dataset = loaded.dataset;

    state.dataset = dataset;
    bootstrapSelection();
    renderAll();
  } catch (error) {
    refs.directionStatsOverall.textContent = "";
    refs.categoryStatsList.textContent = "";
    refs.categoryStatsList.append(
      makeEmpty(
        "Не удалось загрузить ./data/dataset.json. " +
          "Сначала выполните экспорт из базы."
      )
    );
    console.error("UNISCORES UI: dataset load error", error);
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

function bootstrapSelection() {
  const universities = state.dataset?.universities || [];
  if (!universities.length) {
    state.selectedUniversityId = null;
    state.selectedProgramId = null;
    return;
  }

  if (
    state.selectedUniversityId !== null &&
    !universities.some(
      (university) => university.id === state.selectedUniversityId
    )
  ) {
    state.selectedUniversityId = null;
  }

  if (state.selectedUniversityId === null) {
    state.selectedProgramId = null;
    return;
  }

  const programs = getSelectedUniversity()?.programs || [];
  if (!programs.length) {
    state.selectedProgramId = null;
    return;
  }

  if (
    state.selectedProgramId !== null &&
    !programs.some((program) => program.id === state.selectedProgramId)
  ) {
    state.selectedProgramId = null;
    return;
  }
}

function renderAll() {
  updateWizardSteps();
  renderUniversityList();
  renderProgramList();
  renderProgramMeta();
  renderScoreStats();
}

function updateWizardSteps() {
  const university = getSelectedUniversity();
  const hasUniversity = Boolean(university);
  const wasHidden = refs.programStep.classList.contains("is-hidden");

  refs.programStep.classList.toggle("is-hidden", !hasUniversity);
  refs.programStep.setAttribute(
    "aria-hidden",
    hasUniversity ? "false" : "true"
  );

  refs.programSearch.disabled = !hasUniversity;
  refs.backToUniversities.disabled = !hasUniversity;

  if (!hasUniversity) {
    refs.programSearch.value = "";
    refs.programStepCaption.textContent =
      "После выбора вуза выберите направление крупной карточкой.";
    return;
  }

  const universityName = university?.name || "Выбранный вуз";
  refs.programStepCaption.textContent =
    `Вуз: ${universityName}. Теперь выберите направление.`;

  if (wasHidden) {
    triggerStepEnter(refs.programStep);
  }
}

function focusProgramStep() {
  if (refs.programStep.classList.contains("is-hidden")) {
    return;
  }
  refs.programStep.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
  window.setTimeout(() => {
    refs.programSearch.focus();
  }, 220);
}

function focusUniversityStep() {
  refs.universityStep.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
  window.setTimeout(() => {
    refs.universitySearch.focus();
  }, 220);
}

function renderUniversityList() {
  const universities = getFilteredUniversities();
  refs.universityList.textContent = "";

  if (!universities.length) {
    refs.universityList.append(makeEmpty("Вузы не найдены."));
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const [index, university] of universities.entries()) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "list-item";
    applyEnterMotion(button, index, 26, 420);
    if (university.id === state.selectedUniversityId) {
      button.classList.add("active");
    }

    const name = university.name || "Без названия";
    const count = university.programs?.length || 0;

    const title = document.createElement("strong");
    title.textContent = name;
    const subtitle = document.createElement("small");
    subtitle.textContent = `Программ: ${count}`;

    button.append(title, subtitle);
    button.addEventListener("click", () => {
      state.selectedUniversityId = university.id;
      state.selectedProgramId = null;
      refs.programSearch.value = "";
      renderAll();
      focusProgramStep();
    });
    fragment.append(button);
  }

  refs.universityList.append(fragment);
}

function renderProgramList() {
  const university = getSelectedUniversity();
  const programs = getFilteredPrograms();
  refs.programList.textContent = "";

  if (!university) {
    refs.programList.append(makeEmpty("Сначала выберите вуз."));
    clearProgramDependentSections("Сначала выберите направление.");
    return;
  }

  if (!programs.length) {
    refs.programList.append(
      makeEmpty("Для выбранного фильтра программы не найдены.")
    );
    clearProgramDependentSections(
      "Для выбранного фильтра нет доступного направления."
    );
    return;
  }

  if (
    state.selectedProgramId !== null &&
    !programs.some((program) => program.id === state.selectedProgramId)
  ) {
    state.selectedProgramId = null;
  }

  if (state.selectedProgramId === null) {
    clearProgramDependentSections("Выберите направление, чтобы увидеть статистику.");
  }

  const fragment = document.createDocumentFragment();
  for (const [index, program] of programs.entries()) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "list-item";
    applyEnterMotion(button, index, 24, 360);
    if (program.id === state.selectedProgramId) {
      button.classList.add("active");
    }

    const subtitle = [
      program.faculty_name,
      program.payment_type,
      program.program_code,
    ]
      .filter(Boolean)
      .join(" | ");

    const title = document.createElement("strong");
    title.textContent = program.program_name || "Без названия";
    const subtitleNode = document.createElement("small");
    subtitleNode.textContent = subtitle || "Без доп. данных";

    button.append(title, subtitleNode);
    button.addEventListener("click", () => {
      state.selectedProgramId = program.id;
      renderProgramMeta();
      renderScoreStats();
    });

    fragment.append(button);
  }

  refs.programList.append(fragment);
}

function renderProgramMeta() {
  const program = getSelectedProgram();
  refs.programMeta.textContent = "";
  if (!program) {
    refs.programMeta.append(
      makeEmpty("Выберите направление, чтобы посмотреть карточку программы.")
    );
    return;
  }

  const details = [
    ["Факультет", program.faculty_name || "-"],
    ["Специализация", program.specialization_name || "-"],
    ["Форма", program.study_form || "-"],
    ["Оплата", program.payment_type || "-"],
    ["Цена в год", formatMoney(program.annual_fee_som)],
    ["План набора", safeValue(program.admission_plan)],
    ["Порог (осн.)", safeValue(program.threshold_main_score)],
    ["Код", program.program_code || "-"],
  ];

  const fragment = document.createDocumentFragment();
  for (const [index, [label, value]] of details.entries()) {
    const item = document.createElement("div");
    item.className = "meta-item";
    applyEnterMotion(item, index, 24, 160);

    const labelNode = document.createElement("span");
    labelNode.textContent = label;
    const valueNode = document.createElement("strong");
    valueNode.textContent = String(value);

    item.append(labelNode, valueNode);
    fragment.append(item);
  }

  refs.programMeta.append(fragment);
}

function renderScoreStats() {
  const program = getSelectedProgram();
  const round = getSelectedRound();

  if (!program) {
    refs.directionStatsOverall.textContent = "";
    refs.categoryStatsList.textContent = "";
    refs.directionStatsOverall.append(
      makeEmpty("Сначала выберите направление.")
    );
    refs.categoryStatsList.append(makeEmpty("Сначала выберите направление."));
    return;
  }

  renderStatsGrid(
    refs.directionStatsOverall,
    program?.direction_score_stats || null
  );
  renderCategoryStats(round);
}

function renderStatsGrid(container, scoreBundle) {
  container.textContent = "";
  const blocks = buildScoreBlocks(scoreBundle);
  if (!blocks.length) {
    container.append(makeEmpty("Недостаточно данных по баллам."));
    return;
  }

  for (const [index, block] of blocks.entries()) {
    const section = makeScoreBlock(block.title, block.stats);
    applyEnterMotion(section, index, 48, 180);
    container.append(section);
  }
}

function renderCategoryStats(round) {
  refs.categoryStatsList.textContent = "";

  if (!round || !round.categories?.length) {
    refs.categoryStatsList.append(
      makeEmpty("Для единственного тура нет категорий с данными.")
    );
    return;
  }

  const title = document.createElement("h3");
  title.className = "subheading";
  title.textContent = "По каждой категории";
  refs.categoryStatsList.append(title);

  const list = document.createElement("div");
  list.className = "category-grid";

  for (const [index, category] of round.categories.entries()) {
    const card = document.createElement("article");
    card.className = "category-stat-card";
    applyEnterMotion(card, index, 20, 420);

    const heading = document.createElement("h4");
    heading.textContent = category.category_name || "Категория";

    const meta = document.createElement("p");
    meta.className = "category-meta";
    const cutoff = category.cutoff_value;
    const cutoffText =
      cutoff !== null && cutoff !== undefined
        ? `Коэффициент: ${formatScore(cutoff)}`
        : "Коэффициент: -";
    meta.textContent = `${cutoffText} | Строк: ${safeValue(category.rows_count)}`;

    const blocks = buildScoreBlocks(category.score_stats || null);
    if (!blocks.length) {
      card.append(makeEmpty("Недостаточно данных по баллам."));
    } else {
      for (const block of blocks) {
        card.append(makeScoreBlock(block.title, block.stats, true));
      }
    }

    card.prepend(meta);
    card.prepend(heading);
    list.append(card);
  }

  refs.categoryStatsList.append(list);
}

function getMetrics(stats) {
  return [
    [
      "Нижний порог",
      formatScore(stats.lower_passing_score),
    ],
    ["Средний балл", formatScore(stats.average_score)],
    ["Медианный балл", formatScore(stats.median_score)],
    ["Максимальный балл", formatScore(stats.max_score)],
    ["Участников", safeValue(stats.participants_count)],
  ].map(([label, value]) => ({ label, value }));
}

function buildScoreBlocks(scoreBundle) {
  if (!scoreBundle || typeof scoreBundle !== "object") {
    return [];
  }

  const blocks = [];
  for (const kind of SCORE_KIND_CONFIG) {
    if (!isScoreKindVisible(scoreBundle, kind)) {
      continue;
    }

    blocks.push({
      title: kind.title,
      stats: scoreBundle[kind.key],
    });
  }

  return blocks;
}

function isScoreKindVisible(scoreBundle, kind) {
  if (!scoreBundle || !kind) {
    return false;
  }

  if (kind.requiresAdditional && !scoreBundle.has_additional) {
    return false;
  }

  const stats = scoreBundle[kind.key];
  if (!stats || typeof stats !== "object") {
    return false;
  }

  return Number(stats.participants_count) > 0;
}

function makeScoreBlock(title, stats, compact = false) {
  const section = document.createElement("section");
  section.className = compact ? "score-kind compact" : "score-kind";

  const heading = document.createElement("h4");
  heading.className = "score-kind-title";
  heading.textContent = title;

  const metricsGrid = document.createElement("div");
  metricsGrid.className = compact ? "stats-grid compact" : "stats-grid";

  for (const [index, metric] of getMetrics(stats || {}).entries()) {
    const box = document.createElement("div");
    box.className = compact ? "stat-box compact" : "stat-box";
    applyEnterMotion(box, index, 20, 140);

    const labelNode = document.createElement("span");
    labelNode.textContent = metric.label;

    const valueNode = document.createElement("strong");
    valueNode.textContent = metric.value;

    box.append(labelNode, valueNode);
    metricsGrid.append(box);
  }

  section.append(heading, metricsGrid);
  return section;
}

function clearProgramDependentSections(message = "") {
  refs.programMeta.textContent = "";
  refs.directionStatsOverall.textContent = "";
  if (message) {
    refs.directionStatsOverall.append(makeEmpty(message));
  }
  refs.categoryStatsList.textContent = "";
  if (message) {
    refs.categoryStatsList.append(makeEmpty(message));
  }
}

function getFilteredUniversities() {
  const universities = state.dataset?.universities || [];
  const query = refs.universitySearch.value.trim().toLowerCase();
  if (!query) {
    return universities;
  }

  return universities.filter((university) => {
    return (university.name || "").toLowerCase().includes(query);
  });
}

function getFilteredPrograms() {
  const university = getSelectedUniversity();
  if (!university) {
    return [];
  }

  const query = refs.programSearch.value.trim().toLowerCase();
  const programs = university.programs || [];
  if (!query) {
    return programs;
  }

  return programs.filter((program) => {
    const haystack = [
      program.program_name,
      program.specialization_name,
      program.program_code,
      program.faculty_name,
      program.payment_type,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(query);
  });
}

function getSelectedUniversity() {
  const universities = state.dataset?.universities || [];
  return universities.find(
    (university) => university.id === state.selectedUniversityId
  );
}

function getSelectedProgram() {
  const programs = getFilteredPrograms();
  return programs.find((program) => program.id === state.selectedProgramId);
}

function getSelectedRound() {
  const program = getSelectedProgram();
  if (!program) {
    return null;
  }
  return (program.rounds || [])[0] || null;
}

function makeLine(text) {
  const node = document.createElement("span");
  node.textContent = text;
  return node;
}

function formatMoney(value) {
  if (value === null || value === undefined) {
    return "-";
  }
  return `${Number(value).toLocaleString("ru-RU")} сом`;
}

function formatScore(value) {
  if (value === null || value === undefined) {
    return "-";
  }
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
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

function safeValue(value) {
  if (value === null || value === undefined) {
    return "-";
  }
  return String(value);
}

function makeEmpty(text) {
  const element = document.createElement("p");
  element.className = "meta-item";
  applyEnterMotion(element, 0, 0, 0);
  element.textContent = text;
  return element;
}
