// QFlink rule engine console. No framework, no build: the page is served as it is written.

// ------------------------------------------------------------------ helpers

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

/** el('div', {class: 'x'}, child, child) */
function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key === 'text') node.textContent = value;
    else if (key === 'html') node.innerHTML = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2).toLowerCase(), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

const clone = (value) => JSON.parse(JSON.stringify(value));

function toast(message, kind = '') {
  const node = el('div', { class: `toast ${kind}`.trim(), text: message });
  $('#toasts').append(node);
  setTimeout(() => node.remove(), 4200);
}

/** The alert time the engine writes, "yyyy-MM-dd HH:mm:ss" in the job's own zone. */
function parseTime(value, fallback) {
  if (typeof value === 'string') {
    const parsed = Date.parse(value.includes('T') ? value : value.replace(' ', 'T'));
    if (!Number.isNaN(parsed)) return parsed;
  }
  return fallback === undefined ? Date.now() : Date.parse(fallback) || Date.now();
}

const clock = (ms) =>
  new Date(ms).toLocaleTimeString('zh-CN', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });

const number = (value) =>
  Number.isInteger(value) ? String(value) : String(Math.round(value * 1000) / 1000);

// durations, the rule carries milliseconds and nobody thinks in milliseconds
const UNITS = [
  { label: '天', ms: 86400000 },
  { label: '小时', ms: 3600000 },
  { label: '分钟', ms: 60000 },
  { label: '秒', ms: 1000 },
  { label: '毫秒', ms: 1 },
];

function splitDuration(ms) {
  const value = Number(ms) || 0;
  const unit = UNITS.find((u) => value !== 0 && value % u.ms === 0) || UNITS[2];
  return { value: value / unit.ms, unit: unit.ms };
}

function humanDuration(ms) {
  const { value, unit } = splitDuration(ms);
  return `${value}${UNITS.find((u) => u.ms === unit).label}`;
}

// ------------------------------------------------------------------ api

async function api(path, { method = 'GET', body } = {}) {
  const response = await fetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await response.text();
  const payload = text ? JSON.parse(text) : null;
  if (!response.ok && response.status !== 422) {
    throw new Error((payload && payload.error) || `${response.status} ${response.statusText}`);
  }
  return { ok: response.ok, status: response.status, payload };
}

// ------------------------------------------------------------------ state

const state = {
  meta: null,
  rules: [],
  // ! what the console saw since it started, the feeds themselves are capped
  totals: { alert: 0, error: 0, log: 0 },
  alerts: [],
  errors: [],
  logs: [],
  series: new Map(), // "#100 val_cnt" -> {name, slot, points, on}
  draft: null,
  mode: 'form',
  tableView: false,
};

const MAX_FEED = 120;
const MAX_POINTS = 180;
const SERIES_SLOTS = 8;

// ------------------------------------------------------------------ boot

init();

async function init() {
  restoreTheme();
  bindChrome();

  try {
    state.meta = (await api('/api/meta')).payload;
  } catch (error) {
    toast(`读不到 /api/meta：${error.message}`, 'bad');
    return;
  }

  renderMeta();
  await Promise.all([loadRules(), loadHistory(), loadGenerator(), loadDataApis()]);
  await loadStatus();
  connect();
}

function bindChrome() {
  $('#theme-toggle').addEventListener('click', toggleTheme);
  $('#btn-new').addEventListener('click', () => openEditor(null));
  $('#btn-republish').addEventListener('click', republishAll);
  $('#btn-close').addEventListener('click', closeEditor);
  $('#scrim').addEventListener('click', closeEditor);
  $('#btn-example').addEventListener('click', loadExample);
  $('#btn-check').addEventListener('click', () => submit({ save: false }));
  $('#btn-save').addEventListener('click', () => submit({ save: true, publish: false }));
  $('#btn-save-publish').addEventListener('click', () => submit({ save: true, publish: true }));
  $('#mode-form').addEventListener('click', () => setMode('form'));
  $('#mode-json').addEventListener('click', () => setMode('json'));
  $('#btn-gen-start').addEventListener('click', startGenerator);
  $('#btn-gen-stop').addEventListener('click', stopGenerator);
  $('#btn-log-send').addEventListener('click', sendLog);
  $('#log-line').addEventListener('keydown', (event) => event.key === 'Enter' && sendLog());
  $('#btn-table').addEventListener('click', () => {
    state.tableView = !state.tableView;
    $('#btn-table').textContent = state.tableView ? '图表' : '表格';
    renderChart();
  });

  $$('.tab[data-pane]').forEach((tab) =>
    tab.addEventListener('click', () => showPane(tab.dataset.pane)),
  );
  // the tab is in the url, a page which is reopened or shared lands where it was left
  addEventListener('hashchange', () => showPane(location.hash.slice(1)));
  showPane(location.hash.slice(1) || 'alerts');

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !$('#drawer').hidden) closeEditor();
  });

  window.addEventListener('resize', () => {
    if (!$('#pane-chart').hidden) renderChart();
  });
}

function showPane(name) {
  const tab = $$('.tab[data-pane]').find((candidate) => candidate.dataset.pane === name);
  if (!tab) return;

  $$('.tab[data-pane]').forEach((other) =>
    other.setAttribute('aria-selected', String(other === tab)),
  );
  $$('.pane').forEach((pane) => (pane.hidden = pane.id !== `pane-${name}`));
  history.replaceState(null, '', `#${name}`);
  if (name === 'chart') renderChart();
}

function restoreTheme() {
  const saved = localStorage.getItem('qflink-theme');
  if (saved) document.documentElement.setAttribute('data-theme', saved);
}

function toggleTheme() {
  const current =
    document.documentElement.getAttribute('data-theme') ||
    (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  const next = current === 'dark' ? 'light' : 'dark';

  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('qflink-theme', next);
  if (!$('#pane-chart').hidden) renderChart();
}

function renderMeta() {
  const { job_id: jobId, channels, fields } = state.meta;
  $('#job-id').textContent = jobId;
  $('#ch-rule').textContent = channels.rule || '—';
  $('#ch-alert').textContent = channels.alert || '—';

  $('#channels').replaceChildren(
    ...Object.entries(channels).flatMap(([name, description]) => [
      el('dt', { text: name }),
      el('dd', { text: description }),
    ]),
  );

  $('#job-info').replaceChildren(
    ...Object.entries({
      'job.id': jobId,
      'agg.enable': String(state.meta.agg_enabled),
      'log.format': state.meta.log_format,
      'log.field-time': state.meta.log_field_time || '—',
    }).flatMap(([key, value]) => [el('dt', { text: key }), el('dd', { text: value })]),
  );

  const names = Object.keys(fields || {});
  $('#fields').replaceChildren(
    names.length
      ? el(
          'div',
          { class: 'kv' },
          names.map((name) =>
            el('span', { class: 'chip' }, name, el('span', { class: 'n', text: fields[name] })),
          ),
        )
      : el('p', { class: 'help', text: '作业没有配置 etl.output-schema，页面无法提示字段。' }),
  );

  $('#etl-fields').replaceChildren(...names.map((name) => el('option', { value: name })));
  $('#gen-template').value = state.meta.generator_template || '';
  $('#gen-rate').addEventListener('input', renderRateHint);
  renderRateHint();
}

/**
 * A qfile log source reads one line every scan-interval, a generator which runs faster only piles
 * the lines up in the file and the alerts fall further and further behind.
 */
function renderRateHint() {
  const help = $('#gen-rate-help');
  const interval = state.meta.log_scan_interval_ms;

  if (state.meta.log_source !== 'qfile' || !interval) {
    help.textContent = '';
    return;
  }

  const ceiling = 1000 / interval;
  const rate = Number($('#gen-rate').value) || 0;
  help.textContent =
    rate > ceiling
      ? `作业每 ${interval} 毫秒才读一行（log.scan-interval），超过 ${ceiling} 条/秒只会在文件里堆积`
      : `作业每 ${interval} 毫秒读一行（log.scan-interval），上限约 ${ceiling} 条/秒`;
  help.style.color = rate > ceiling ? 'var(--serious)' : '';
}

// ------------------------------------------------------------------ events

function connect() {
  const source = new EventSource('/api/events');
  const status = $('#conn');

  source.addEventListener('open', () => {
    status.dataset.state = 'open';
    $('#conn-text').textContent = '实时连接';
  });
  source.addEventListener('error', () => {
    status.dataset.state = 'closed';
    $('#conn-text').textContent = '已断开，重连中';
  });

  // ! every frame arrives under one name, an "error" frame would be indistinguishable from the
  // ! failure event the event source raises on its own
  source.addEventListener('qflink', (frame) => {
    const event = JSON.parse(frame.data);
    switch (event.type) {
      case 'alert':
        return onAlert(event);
      case 'error':
        return onError(event);
      case 'log':
        return onLog(event);
      case 'rule':
        return loadRules();
      case 'status': {
        const payload = event.payload || {};
        if (payload.error) toast(`生成器停止：${payload.error}`, 'bad');
        return loadGenerator();
      }
      default:
        return undefined;
    }
  });
}

async function loadHistory() {
  const [alerts, errors, logs] = await Promise.all([
    api('/api/history?type=alert&limit=200'),
    api('/api/history?type=error&limit=50'),
    api('/api/history?type=log&limit=30'),
  ]);

  alerts.payload.forEach((event) => onAlert(event, true));
  errors.payload.forEach((event) => onError(event, true));
  logs.payload.forEach((event) => onLog(event, true));

  renderAlerts();
  renderErrors();
  renderLogs();
  renderChart();
}

/** The counters of the console itself, the replayed history is already in them. */
async function loadStatus() {
  const { payload } = await api('/api/status');
  state.totals = { alert: 0, error: 0, log: 0, ...payload.totals };
  updateStats();
}

function onAlert(event, quiet = false) {
  const alert = event.payload || {};
  const at = parseTime(alert.alert_time, event.time);

  state.alerts.unshift({ at, alert });
  state.alerts.length = Math.min(state.alerts.length, MAX_FEED);

  for (const [metric, value] of Object.entries(alert.metrics || {})) {
    if (typeof value !== 'number') continue;
    const name = `#${alert.rule_id} ${metric}`;

    let serie = state.series.get(name);
    if (!serie) {
      serie = { name, slot: state.series.size, points: [], on: state.series.size < SERIES_SLOTS };
      state.series.set(name, serie);
    }
    serie.points.push({ at, value });
    if (serie.points.length > MAX_POINTS) serie.points.shift();
  }

  if (!quiet) {
    state.totals.alert += 1;
    renderAlerts();
    updateStats();
    if (!$('#pane-chart').hidden) renderChart();
  }
}

function onError(event, quiet = false) {
  state.errors.unshift({ at: Date.parse(event.time) || Date.now(), error: event.payload || {} });
  state.errors.length = Math.min(state.errors.length, MAX_FEED);
  if (!quiet) {
    state.totals.error += 1;
    renderErrors();
    updateStats();
  }
}

function onLog(event, quiet = false) {
  state.logs.unshift({ at: Date.parse(event.time) || Date.now(), ...(event.payload || {}) });
  state.logs.length = Math.min(state.logs.length, 40);
  if (!quiet) {
    state.totals.log += 1;
    renderLogs();
    updateStats();
  }
}

function updateStats() {
  const published = state.rules.filter(
    (entry) => entry.published_state && entry.published_state !== 'REMOVE',
  ).length;

  $('#stat-rules').textContent = state.rules.length;
  $('#stat-rules-hint').textContent = `生效中 ${published}`;
  $('#stat-alerts').textContent = state.totals.alert;
  $('#stat-alerts-hint').textContent = state.alerts.length
    ? `最近 ${clock(state.alerts[0].at)}`
    : '等待第一条';
  $('#stat-errors').textContent = state.totals.error;
  $('#stat-logs').textContent = state.totals.log;
}

// ------------------------------------------------------------------ rules

async function loadRules() {
  state.rules = (await api('/api/rules')).payload;
  renderRules();
  updateStats();
}

/** draft: never published, live: published and not removed, off: removed from the job. */
function ruleState(entry) {
  if (!entry.published_state) return 'draft';
  return entry.published_state === 'REMOVE' ? 'off' : 'live';
}

function renderRules() {
  const list = $('#rule-list');
  $('#rule-count').textContent = state.rules.length ? `共 ${state.rules.length} 条` : '';

  if (!state.rules.length) {
    list.replaceChildren(
      el('p', {
        class: 'empty',
        text: '还没有规则。点右上角「新建规则」，或者先载入示例规则。',
      }),
    );
    return;
  }

  list.replaceChildren(
    ...state.rules.map((entry) => {
      const rule = entry.rule || {};
      const status = ruleState(entry);
      const label = { draft: '草稿', live: '生效中', off: '已下线' }[status];
      const window = rule.window || {};

      return el(
        'article',
        { class: `rule is-${status}` },
        el(
          'div',
          { class: 'rule-top' },
          el('span', { class: 'rule-id', text: `#${rule.rule_id}` }),
          el('span', { class: 'badge', 'data-state': status }, el('i', { class: 'swatch' }), label),
          el('span', {
            class: 'sub',
            text: entry.published_at ? `下发于 ${clock(Date.parse(entry.published_at))}` : '未下发',
          }),
        ),
        el(
          'div',
          { class: 'rule-summary' },
          el('div', {}, el('span', { class: 'k', text: '分组' }), (rule.keys || []).join(', ') || '—'),
          el(
            'div',
            {},
            el('span', { class: 'k', text: '窗口' }),
            window.type
              ? `${window.type} ${humanDuration(window.size)}${
                  window.step ? ` / ${humanDuration(window.step)}` : ''
                } · ${window.trigger || ''}`
              : '—',
          ),
          el(
            'div',
            {},
            el('span', { class: 'k', text: '聚合' }),
            (rule.aggregates || [])
              .map((agg) => `${agg.name}=${agg.method}(${(agg.inputs || []).join(',')})`)
              .join(', ') || '—',
          ),
          el(
            'div',
            {},
            el('span', { class: 'k', text: '阈值' }),
            el('code', { text: (rule.threshold && rule.threshold.expr) || '—' }),
          ),
        ),
        el(
          'div',
          { class: 'rule-actions' },
          el('button', { class: 'sm', onClick: () => openEditor(entry) }, '编辑'),
          status === 'live'
            ? el('button', { class: 'sm', onClick: () => publish(rule.rule_id, 'REMOVE') }, '下线')
            : el(
                'button',
                { class: 'sm primary', onClick: () => publish(rule.rule_id, 'ACTIVE') },
                status === 'off' ? '重新上线' : '下发',
              ),
          el(
            'button',
            { class: 'sm', onClick: () => publish(rule.rule_id, 'UPDATE', true) },
            '清空状态并下发',
          ),
          el('button', { class: 'sm danger', onClick: () => removeRule(rule.rule_id) }, '删除'),
        ),
      );
    }),
  );
}

async function publish(ruleId, stateName, clearState = false) {
  try {
    await api(`/api/rules/${ruleId}/publish`, {
      method: 'POST',
      body: { state: stateName, clear_state: clearState },
    });
    toast(`规则 #${ruleId} 已以 ${stateName} 下发`, 'good');
    await loadRules();
  } catch (error) {
    toast(error.message, 'bad');
  }
}

async function removeRule(ruleId) {
  if (!confirm(`删除规则 #${ruleId}？会先向规则通道发一条 REMOVE。`)) return;
  try {
    await api(`/api/rules/${ruleId}`, { method: 'DELETE' });
    toast(`规则 #${ruleId} 已删除`, 'good');
    await loadRules();
  } catch (error) {
    toast(error.message, 'bad');
  }
}

async function republishAll() {
  try {
    const { payload } = await api('/api/rules/publish-all', { method: 'POST' });
    toast(`已重新下发 ${payload.length} 条规则`, 'good');
    await loadRules();
  } catch (error) {
    toast(error.message, 'bad');
  }
}

// ------------------------------------------------------------------ editor

function emptyRule() {
  return {
    rule_id: null,
    job_id: state.meta.job_id,
    rule_state: 'ACTIVE',
    clear_state: false,
    filter: { expr: '', params: [] },
    flat_map: { pattern: '', normal_fields: [], mapping_fields: [] },
    keys: [],
    window: { type: 'TUMBLE', trigger: 'SINGLE', size: 60000, step: 0, offset: 0 },
    aggregates: [{ name: '', inputs: [''], method: 'COUNT' }],
    threshold: { expr: '', params: [] },
    extras: {},
  };
}

function openEditor(entry) {
  state.draft = entry ? clone(entry.rule) : emptyRule();
  $('#drawer-title').textContent = entry ? `编辑规则 #${state.draft.rule_id}` : '新建规则';
  $('#report').replaceChildren();
  setMode('form', false);
  $('#drawer').hidden = false;
  $('#scrim').hidden = false;
}

function closeEditor() {
  $('#drawer').hidden = true;
  $('#scrim').hidden = true;
  state.draft = null;
}

function loadExample() {
  state.draft = clone(state.meta.example_rule || emptyRule());
  state.draft.job_id = state.meta.job_id;
  setMode(state.mode, false);
  toast('已载入示例规则，保存前记得改 rule_id');
}

function setMode(mode, sync = true) {
  // read whatever the current mode holds, so switching never loses an edit
  if (sync && state.draft) {
    try {
      state.draft = state.mode === 'json' && !$('#rule-json').hidden ? readJson() : readForm();
    } catch (error) {
      toast(error.message, 'bad');
      return;
    }
  }

  state.mode = mode;
  $('#mode-form').setAttribute('aria-selected', String(mode === 'form'));
  $('#mode-json').setAttribute('aria-selected', String(mode === 'json'));
  $('#rule-form').hidden = mode !== 'form';
  $('#rule-json').hidden = mode !== 'json';

  if (mode === 'form') buildForm(state.draft);
  else $('#json-text').value = JSON.stringify(state.draft, null, 2);
}

function readJson() {
  try {
    return JSON.parse($('#json-text').value);
  } catch (error) {
    throw new Error(`JSON 格式不对：${error.message}`);
  }
}

// ---- the form

function field(label, control, help) {
  return el(
    'div',
    { class: 'field' },
    el('label', { text: label }),
    control,
    help ? el('span', { class: 'help', html: help }) : null,
  );
}

function select(className, values, current) {
  return el(
    'select',
    { class: className },
    values.map((value) => el('option', { value, selected: value === current }, value)),
  );
}

function input(className, value, attrs = {}) {
  return el('input', { class: className, type: 'text', value: value ?? '', ...attrs });
}

function durationRow(className, ms) {
  const { value, unit } = splitDuration(ms);
  return el(
    'div',
    { class: 'row' },
    el('input', { class: `${className}-value`, type: 'number', min: '0', step: '1', value }),
    el(
      'select',
      { class: `${className}-unit fit` },
      UNITS.map((u) => el('option', { value: u.ms, selected: u.ms === unit }, u.label)),
    ),
  );
}

function readDuration(className) {
  const value = Number($(`.${className}-value`).value || 0);
  const unit = Number($(`.${className}-unit`).value || 1);
  return Math.round(value * unit);
}

function rowsBlock(id, rows, onAdd, addLabel) {
  return el(
    'div',
    {},
    el('div', { class: 'sub-rows', id }, rows),
    el('button', { class: 'sm', type: 'button', onClick: onAdd }, addLabel),
  );
}

/** A comma separated list with the fields of the job offered underneath. */
function fieldList(className, values, extra = []) {
  const control = input(className, (values || []).join(', '), {
    placeholder: '逗号分隔',
  });
  const names = [...extra, ...Object.keys(state.meta.fields || {})];

  return el(
    'div',
    {},
    control,
    names.length
      ? el(
          'div',
          { class: 'kv' },
          names.map((name) =>
            el(
              'button',
              {
                class: 'chip',
                type: 'button',
                onClick: () => {
                  const current = control.value
                    .split(',')
                    .map((part) => part.trim())
                    .filter(Boolean);
                  if (!current.includes(name)) current.push(name);
                  control.value = current.join(', ');
                },
              },
              name,
            ),
          ),
        )
      : null,
  );
}

const parseList = (value) =>
  value
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);

function buildForm(rule) {
  const enums = state.meta.enums;
  const form = $('#rule-form');
  const flat = rule.flat_map || {};

  const normalRows = (flat.normal_fields || []).map((nf, index) => normalFieldRow(nf, index));
  const mappingRows = (flat.mapping_fields || []).map((mf) => mappingFieldRow(mf));
  const aggRows = (rule.aggregates || []).map((agg) => aggregateRow(agg));
  const extraRows = Object.entries(rule.extras || {}).map(([key, value]) => extraRow(key, value));

  form.replaceChildren(
    el(
      'fieldset',
      {},
      el('legend', { text: '基本' }),
      el(
        'div',
        { class: 'row' },
        field(
          'rule_id',
          el('input', {
            class: 'f-rule-id',
            type: 'number',
            min: '1',
            value: rule.rule_id ?? '',
            placeholder: '留空自动分配',
          }),
        ),
        field('job_id', input('f-job-id', rule.job_id || state.meta.job_id)),
        field('rule_state', select('f-rule-state', enums.rule_state, rule.rule_state || 'ACTIVE')),
      ),
    ),

    el(
      'fieldset',
      {},
      el('legend', { text: '过滤（作用在原始日志上）' }),
      field(
        '表达式',
        input('f-filter-expr', (rule.filter || {}).expr, {
          placeholder: "string.contains(raw, 'stdout')",
        }),
        'Aviator 表达式，留空表示不过滤。',
      ),
      field(
        '参数字段',
        fieldList('f-filter-params', (rule.filter || {}).params, ['raw']),
        '表达式里读到的字段都要列出来，引擎只把这些字段塞进求值环境。',
      ),
    ),

    el(
      'fieldset',
      {},
      el('legend', { text: '解析 flat_map' }),
      field(
        '正则',
        input('f-pattern', flat.pattern, { placeholder: '(\\d{4}-\\d{2}-\\d{2} .*?) (.*)' }),
        '捕获组按顺序对应下面的普通字段，raw 格式的日志才需要。',
      ),
      el('label', { class: 'help', text: '普通字段（顺序 = 捕获组顺序）' }),
      rowsBlock(
        'f-normal',
        normalRows,
        () => {
          const draft = readForm();
          draft.flat_map.normal_fields.push({ name: '', type: 'STRING', default: null });
          state.draft = draft;
          buildForm(draft);
        },
        '+ 普通字段',
      ),
      el('div', { style: 'height:10px' }),
      el('label', { class: 'help', text: '映射字段（在解析结果上再算一层）' }),
      rowsBlock(
        'f-mapping',
        mappingRows,
        () => {
          const draft = readForm();
          draft.flat_map.mapping_fields.push({ name: '', type: 'STRING', expr: '', default: null });
          state.draft = draft;
          buildForm(draft);
        },
        '+ 映射字段',
      ),
    ),

    el(
      'fieldset',
      {},
      el('legend', { text: '聚合' }),
      field('分组字段 keys', fieldList('f-keys', rule.keys), '多个字段用逗号分隔。'),
      el(
        'div',
        { class: 'row' },
        field('窗口类型', select('f-win-type', enums.window_type, (rule.window || {}).type)),
        field('触发', select('f-win-trigger', enums.trigger, (rule.window || {}).trigger)),
      ),
      el(
        'div',
        { class: 'row' },
        field('大小 size', durationRow('f-win-size', (rule.window || {}).size)),
        field('步长 step', durationRow('f-win-step', (rule.window || {}).step)),
        field('偏移 offset', durationRow('f-win-offset', (rule.window || {}).offset)),
      ),
      el('p', {
        class: 'help',
        text: 'SLIDE / CUMULATE 的 size 必须是 step 的整数倍，窗口才切得成分片；TUMBLE 不用填 step。',
      }),
      el('div', { style: 'height:10px' }),
      rowsBlock(
        'f-aggs',
        aggRows,
        () => {
          const draft = readForm();
          draft.aggregates.push({ name: '', inputs: [''], method: 'COUNT' });
          state.draft = draft;
          buildForm(draft);
        },
        '+ 聚合',
      ),
    ),

    el(
      'fieldset',
      {},
      el('legend', { text: '阈值' }),
      field('表达式', input('f-th-expr', (rule.threshold || {}).expr, { placeholder: 'val_cnt > 1' })),
      field(
        '参数',
        fieldList(
          'f-th-params',
          (rule.threshold || {}).params,
          (rule.aggregates || []).map((agg) => agg.name).filter(Boolean),
        ),
        '只能引用上面聚合的名字，它们就是告警里的 metrics。',
      ),
    ),

    el(
      'fieldset',
      {},
      el('legend', { text: 'extras' }),
      rowsBlock(
        'f-extras',
        extraRows,
        () => {
          const draft = readForm();
          draft.extras[''] = '';
          state.draft = draft;
          buildForm(draft);
        },
        '+ 键值',
      ),
      el('p', { class: 'help', text: '透传给告警的上下文，例如 etl.table 决定 ETL 结果写到哪个表。' }),
    ),
  );
}

function removeButton(onRemove) {
  return el('button', { class: 'sm danger fit', type: 'button', onClick: onRemove }, '删除');
}

function normalFieldRow(nf, index) {
  const isJson = nf.type === 'JSON';
  const paths = nf.json_paths || [];

  const row = el(
    'div',
    { class: 'sub-row nf-row', 'data-json': String(isJson) },
    field('名称', input('nf-name', nf.name, { placeholder: isJson ? '（JSON 组不用名称）' : 'time' })),
    field('类型', select('nf-type', state.meta.enums.normal_field_type, nf.type || 'STRING')),
    field('默认值', input('nf-default', nf.default ?? '')),
    el(
      'div',
      { class: 'fit', style: 'display:flex;gap:6px' },
      el('button', { class: 'sm', type: 'button', onClick: () => moveNormal(index, -1) }, '↑'),
      el('button', { class: 'sm', type: 'button', onClick: () => moveNormal(index, 1) }, '↓'),
      removeButton(() => {
        const draft = readForm();
        draft.flat_map.normal_fields.splice(index, 1);
        state.draft = draft;
        buildForm(draft);
      }),
    ),
  );

  $('.nf-type', row).addEventListener('change', () => {
    const draft = readForm();
    const target = draft.flat_map.normal_fields[index];
    if (target.type === 'JSON' && !target.json_paths) {
      target.json_paths = [{ name: '', expr: '$.', type: 'STRING', default: null }];
    }
    state.draft = draft;
    buildForm(draft);
  });

  if (!isJson) return row;

  return el(
    'div',
    { class: 'sub-row nf-row', 'data-json': 'true', style: 'grid-template-columns:1fr' },
    row,
    el('label', { class: 'help', text: 'JSON 组：整段用 json path 拆成多个字段' }),
    el(
      'div',
      { class: 'sub-rows jp-rows' },
      paths.map((path, pathIndex) =>
        el(
          'div',
          { class: 'sub-row jp-row' },
          field('名称', input('jp-name', path.name, { placeholder: 'key1' })),
          field('json path', input('jp-expr', path.expr, { placeholder: '$.key1' })),
          field('类型', select('jp-type', state.meta.enums.field_type, path.type || 'STRING')),
          field('默认值', input('jp-default', path.default ?? '')),
          removeButton(() => {
            const draft = readForm();
            draft.flat_map.normal_fields[index].json_paths.splice(pathIndex, 1);
            state.draft = draft;
            buildForm(draft);
          }),
        ),
      ),
    ),
    el(
      'button',
      {
        class: 'sm',
        type: 'button',
        onClick: () => {
          const draft = readForm();
          const target = draft.flat_map.normal_fields[index];
          (target.json_paths = target.json_paths || []).push({
            name: '',
            expr: '$.',
            type: 'STRING',
            default: null,
          });
          state.draft = draft;
          buildForm(draft);
        },
      },
      '+ json path',
    ),
  );
}

function moveNormal(index, delta) {
  const draft = readForm();
  const fields = draft.flat_map.normal_fields;
  const target = index + delta;
  if (target < 0 || target >= fields.length) return;

  [fields[index], fields[target]] = [fields[target], fields[index]];
  state.draft = draft;
  buildForm(draft);
}

function mappingFieldRow(mf) {
  return el(
    'div',
    { class: 'sub-row mf-row' },
    field('名称', input('mf-name', mf.name, { placeholder: 'is_odd' })),
    field('类型', select('mf-type', state.meta.enums.field_type, mf.type || 'STRING')),
    field('表达式', input('mf-expr', mf.expr, { placeholder: 'key1 % 2 == 1' })),
    field('默认值', input('mf-default', mf.default ?? '')),
    removeButton((event) => {
      const row = event.target.closest('.mf-row');
      const index = $$('.mf-row').indexOf(row);
      const draft = readForm();
      draft.flat_map.mapping_fields.splice(index, 1);
      state.draft = draft;
      buildForm(draft);
    }),
  );
}

function aggregateRow(agg) {
  return el(
    'div',
    { class: 'sub-row agg-row' },
    field('名称', input('agg-name', agg.name, { placeholder: 'val_cnt' })),
    field('方法', select('agg-method', state.meta.enums.method, agg.method || 'COUNT')),
    field(
      '输入字段',
      input('agg-input', (agg.inputs || [])[0] || '', { placeholder: 'key2', list: 'etl-fields' }),
    ),
    field('过滤表达式', input('agg-filter', (agg.filter || {}).expr, { placeholder: '可选' })),
    removeButton((event) => {
      const row = event.target.closest('.agg-row');
      const index = $$('.agg-row').indexOf(row);
      const draft = readForm();
      draft.aggregates.splice(index, 1);
      state.draft = draft;
      buildForm(draft);
    }),
  );
}

function extraRow(key, value) {
  return el(
    'div',
    { class: 'sub-row ex-row' },
    field('键', input('ex-key', key)),
    field('值', input('ex-val', value)),
    removeButton((event) => {
      const row = event.target.closest('.ex-row');
      const index = $$('.ex-row').indexOf(row);
      const draft = readForm();
      draft.extras = Object.fromEntries(Object.entries(draft.extras).filter((_, i) => i !== index));
      state.draft = draft;
      buildForm(draft);
    }),
  );
}

/** The empty string is not a value, a blank default means "no default". */
const orNull = (value) => (value === '' || value === undefined ? null : value);

/**
 * The default of a field is typed like the field itself: a text input alone would turn the default
 * 0 of an INT into the string "0", which is not what the engine would put in the row.
 */
function typedDefault(value, type) {
  const raw = (value || '').trim();
  if (raw === '') return null;

  switch (type) {
    case 'INT':
    case 'BIGINT':
      return Number.isInteger(Number(raw)) ? Number(raw) : raw;
    case 'FLOAT':
    case 'DOUBLE':
      return Number.isNaN(Number(raw)) ? raw : Number(raw);
    case 'BOOLEAN':
      if (raw === 'true') return true;
      if (raw === 'false') return false;
      return raw;
    case 'MAP':
    case 'LIST':
      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    default:
      return raw;
  }
}

function readForm() {
  if ($('#rule-form').hidden) return state.draft;

  const ruleId = $('.f-rule-id').value;
  const filterExpr = $('.f-filter-expr').value.trim();
  const thresholdExpr = $('.f-th-expr').value.trim();

  const normalFields = $$('#f-normal > .nf-row').map((wrapper) => {
    const row = $('.nf-row', wrapper) || wrapper;
    const type = $('.nf-type', row).value;
    const field = {
      name: $('.nf-name', row).value.trim(),
      type,
      default: orNull($('.nf-default', row).value.trim()),
    };

    if (type === 'JSON') {
      field.json_paths = $$('.jp-row', wrapper).map((path) => ({
        name: $('.jp-name', path).value.trim(),
        expr: $('.jp-expr', path).value.trim(),
        type: $('.jp-type', path).value,
        default: typedDefault($('.jp-default', path).value, $('.jp-type', path).value),
      }));
    }
    return field;
  });

  const rule = {
    rule_id: ruleId ? Number(ruleId) : null,
    job_id: $('.f-job-id').value.trim(),
    rule_state: $('.f-rule-state').value,
    clear_state: false,
    flat_map: {
      pattern: $('.f-pattern').value,
      normal_fields: normalFields,
      mapping_fields: $$('.mf-row').map((row) => ({
        name: $('.mf-name', row).value.trim(),
        type: $('.mf-type', row).value,
        expr: $('.mf-expr', row).value.trim(),
        default: typedDefault($('.mf-default', row).value, $('.mf-type', row).value),
      })),
    },
    keys: parseList($('.f-keys').value),
    window: {
      type: $('.f-win-type').value,
      trigger: $('.f-win-trigger').value,
      size: readDuration('f-win-size'),
      step: readDuration('f-win-step'),
      offset: readDuration('f-win-offset'),
    },
    aggregates: $$('.agg-row').map((row) => {
      const aggregate = {
        name: $('.agg-name', row).value.trim(),
        inputs: parseList($('.agg-input', row).value),
        method: $('.agg-method', row).value,
      };
      const filter = $('.agg-filter', row).value.trim();
      if (filter) aggregate.filter = { expr: filter, params: parseList($('.agg-input', row).value) };
      return aggregate;
    }),
    extras: Object.fromEntries(
      $$('.ex-row')
        .map((row) => [$('.ex-key', row).value.trim(), $('.ex-val', row).value])
        .filter(([key]) => key),
    ),
  };

  if (filterExpr) rule.filter = { expr: filterExpr, params: parseList($('.f-filter-params').value) };
  if (thresholdExpr) {
    rule.threshold = { expr: thresholdExpr, params: parseList($('.f-th-params').value) };
  }
  if (rule.rule_id === null) delete rule.rule_id;

  return rule;
}

async function submit({ save, publish: shouldPublish }) {
  let rule;
  try {
    rule = state.mode === 'json' ? readJson() : readForm();
  } catch (error) {
    toast(error.message, 'bad');
    return;
  }
  state.draft = rule;

  const path = save ? `/api/rules?publish=${Boolean(shouldPublish)}` : '/api/rules/validate';
  try {
    const { payload } = await api(path, { method: 'POST', body: rule });
    showReport(payload);

    if (payload.valid && save) {
      toast(shouldPublish ? '已保存并下发' : '已保存草稿', 'good');
      state.draft = payload.entry ? payload.entry.rule : rule;
      await loadRules();
      if (state.mode === 'form') buildForm(state.draft);
      else $('#json-text').value = JSON.stringify(state.draft, null, 2);
    } else if (payload.valid) {
      toast('校验通过', 'good');
    }
  } catch (error) {
    toast(error.message, 'bad');
  }
}

function showReport(report) {
  const messages = [
    ...(report.errors || []).map((text) => ({ kind: 'err', icon: '✕', text })),
    ...(report.warnings || []).map((text) => ({ kind: 'warn', icon: '!', text })),
  ];
  if (!messages.length && report.valid) {
    messages.push({ kind: 'ok', icon: '✓', text: '规则没有问题。' });
  }

  $('#report').replaceChildren(
    ...messages.map((message) =>
      el(
        'div',
        { class: `msg ${message.kind}` },
        el('span', { class: 'icon', text: message.icon }),
        el('span', { text: message.text }),
      ),
    ),
  );
}

// ------------------------------------------------------------------ feeds

function renderAlerts() {
  const feed = $('#alert-feed');
  if (!state.alerts.length) {
    feed.replaceChildren(
      el('p', { class: 'empty', text: '还没有告警。先下发一条规则，再往日志通道里发点数据。' }),
    );
    return;
  }

  feed.replaceChildren(
    ...state.alerts.map(({ at, alert }) =>
      el(
        'div',
        { class: 'feed-row' },
        el('span', { class: 'when', text: clock(at) }),
        el(
          'div',
          { class: 'what' },
          el(
            'div',
            {},
            el('b', { text: `#${alert.rule_id}` }),
            ' ',
            el('span', {
              class: 'sub',
              text: Object.entries(alert.keys || {})
                .map(([key, value]) => `${key}=${value}`)
                .join(' · '),
            }),
          ),
          el(
            'div',
            { class: 'kv' },
            Object.entries(alert.metrics || {}).map(([name, value]) =>
              el('span', { class: 'chip' }, name, el('span', { class: 'n', text: number(value) })),
            ),
          ),
        ),
      ),
    ),
  );
}

function renderErrors() {
  const feed = $('#error-feed');
  if (!state.errors.length) {
    feed.replaceChildren(
      el('p', { class: 'empty', text: '没有错误。规则解析失败、表达式报错都会出现在这里。' }),
    );
    return;
  }

  feed.replaceChildren(
    ...state.errors.map(({ at, error }) =>
      el(
        'div',
        { class: 'feed-row error' },
        el('span', { class: 'when', text: clock(at) }),
        el(
          'div',
          { class: 'what' },
          el('div', { class: 'line', text: error.error || error.raw || '未知错误' }),
          error.data ? el('div', { class: 'sub', text: `数据 ${error.data}` }) : null,
          error.rule ? el('div', { class: 'sub', text: `规则 ${error.rule}` }) : null,
        ),
      ),
    ),
  );
}

function renderLogs() {
  const feed = $('#log-feed');
  if (!state.logs.length) {
    feed.replaceChildren(el('p', { class: 'empty', text: '还没有发出日志。' }));
    return;
  }

  feed.replaceChildren(
    ...state.logs.map((entry) =>
      el(
        'div',
        { class: 'feed-row' },
        el('span', { class: 'when', text: clock(entry.at) }),
        el('div', { class: 'line', text: entry.line }),
      ),
    ),
  );
}

// ------------------------------------------------------------------ data service

async function loadDataApis() {
  try {
    const { payload } = await api('/api/data');
    renderDataApis(payload);
  } catch (error) {
    $('#data-apis').replaceChildren(
      el('p', { class: 'empty', text: `读不到数据服务：${error.message}` }),
    );
  }
}

function renderDataApis({ prefix, apis }) {
  if (!apis.length) {
    $('#data-apis').replaceChildren(el('p', { class: 'empty', text: '还没有定义任何接口。' }));
    return;
  }
  $('#data-apis').replaceChildren(...apis.map((definition) => dataApiCard(prefix, definition)));
}

/** One api: what it is, a form for its parameters and what it answered. */
function dataApiCard(prefix, definition) {
  const url = prefix + definition.path;
  const result = el('pre', { class: 'data-result', hidden: true });
  const status = el('span', { class: 'badge', hidden: true });

  const isTemplate = definition.kind === 'template';
  const inputs = isTemplate ? templateInputs(definition.params) : null;
  const body = isTemplate
    ? null
    : el('textarea', { rows: 6, spellcheck: 'false' }, exampleRequest(definition));

  async function call() {
    const target = isTemplate ? `${url}?${new URLSearchParams(readInputs(inputs))}` : url;
    const response = await fetch(target, {
      method: definition.method,
      headers: isTemplate ? undefined : { 'Content-Type': 'application/json' },
      body: isTemplate ? undefined : body.value,
    });
    const payload = await response.json().catch(() => null);
    status.textContent = `${response.status} ${response.ok ? '成功' : '失败'}`;
    status.hidden = false;
    result.textContent = JSON.stringify(payload, null, 2);
    result.hidden = false;
  }

  return el(
    'fieldset',
    { class: 'data-api' },
    el('legend', {}, el('span', { class: 'mono' }, `${definition.method} ${url}`)),
    el('p', { class: 'help', text: definition.description || definition.name }),
    isTemplate
      ? [
          ...inputs.map(({ field: node }) => node),
          el(
            'details',
            {},
            el('summary', { class: 'help', text: 'SQL 模板' }),
            el('pre', { class: 'data-result', text: definition.sql }),
          ),
        ]
      : el(
          'div',
          { class: 'field' },
          el('label', { text: '请求体' }),
          body,
          el('span', {
            class: 'help',
            text: `可用字段：${definition.fields.join('、')}；每页最多 ${definition.max_limit} 条`,
          }),
        ),
    el(
      'div',
      { class: 'row' },
      el('div', {}, status),
      el('button', { class: 'primary fit', onclick: () => call().catch((e) => toast(e.message, 'bad')) }, '调用'),
    ),
    result,
  );
}

/** A select for a whitelisted parameter, a text box for the others, prefilled with the default. */
function templateInputs(params) {
  return Object.entries(params || {}).map(([name, param]) => {
    const control = param.allowed
      ? select('', param.allowed, param.default)
      : input('', param.default ?? '', { placeholder: param.kind === 'NUMBER' ? '整数' : '' });
    const hint =
      param.kind === 'IDENTIFIER'
        ? '白名单，原样写进 SQL'
        : param.kind === 'NUMBER'
          ? `绑定的整数，最大 ${param.max}`
          : '绑定的值';
    return { name, control, field: field(name, control, hint) };
  });
}

function readInputs(inputs) {
  return Object.fromEntries(
    inputs.filter(({ control }) => control.value !== '').map(({ name, control }) => [name, control.value]),
  );
}

/** A unified request ordered by the first field, newest first, one page. */
function exampleRequest(definition) {
  return JSON.stringify(
    { order_by: [{ order_field: definition.fields[0], order_type: 'DESC' }], limit: 10, offset: 0 },
    null,
    2,
  );
}

// ------------------------------------------------------------------ generator

async function loadGenerator() {
  const { payload } = await api('/api/generator');
  applyGenerator(payload);
}

function applyGenerator(status) {
  if (status.template) $('#gen-template').value = status.template;
  $('#gen-rate').value = status.rate;
  renderRateHint();
  $('#btn-gen-start').textContent = status.running ? '按新设置重启' : '开始';
  $('#stat-logs-hint').textContent = status.running
    ? `生成器 ${status.rate}/秒，已发 ${status.sent}`
    : `生成器已停止，已发 ${status.sent}`;
}

async function startGenerator() {
  try {
    const { payload } = await api('/api/generator/start', {
      method: 'POST',
      body: { template: $('#gen-template').value, rate: Number($('#gen-rate').value) },
    });
    applyGenerator(payload);
    toast('生成器已启动', 'good');
  } catch (error) {
    toast(error.message, 'bad');
  }
}

async function stopGenerator() {
  const { payload } = await api('/api/generator/stop', { method: 'POST' });
  applyGenerator(payload);
  toast('生成器已停止');
}

async function sendLog() {
  const line = $('#log-line').value.trim();
  if (!line) return;

  try {
    await api('/api/logs', { method: 'POST', body: { line } });
    $('#log-line').value = '';
  } catch (error) {
    toast(error.message, 'bad');
  }
}

// ------------------------------------------------------------------ chart

const SVG_NS = 'http://www.w3.org/2000/svg';

function svg(tag, props = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === null || value === undefined) continue;
    node.setAttribute(key, value);
  }
  return node;
}

function visibleSeries() {
  return [...state.series.values()].filter((serie) => serie.slot < SERIES_SLOTS);
}

function renderChart() {
  renderLegend();
  const host = $('#chart');
  const table = $('#chart-table');

  table.hidden = !state.tableView;
  host.hidden = state.tableView;
  if (state.tableView) {
    renderChartTable();
    return;
  }

  const series = visibleSeries().filter((serie) => serie.on && serie.points.length);
  if (!series.length) {
    host.replaceChildren(
      el('p', { class: 'empty', text: '还没有指标。每条告警的 metrics 会画成一条线。' }),
    );
    return;
  }

  const width = Math.max(host.clientWidth || 640, 420);
  const height = 300;
  // the line end labels need room, they are what identifies a series without its colour
  const pad = { top: 14, right: series.length <= 4 ? 104 : 20, bottom: 26, left: 46 };

  const times = series.flatMap((serie) => serie.points.map((point) => point.at));
  const values = series.flatMap((serie) => serie.points.map((point) => point.value));
  const minAt = Math.min(...times);
  const maxAt = Math.max(...times);
  const maxValue = niceMax(Math.max(...values));

  const x = (at) =>
    pad.left + (maxAt === minAt ? 0 : ((at - minAt) / (maxAt - minAt)) * (width - pad.left - pad.right));
  const y = (value) =>
    height - pad.bottom - (maxValue === 0 ? 0 : (value / maxValue) * (height - pad.top - pad.bottom));

  const root = svg('svg', {
    viewBox: `0 0 ${width} ${height}`,
    role: 'img',
    'aria-label': '告警指标随时间的变化',
  });

  // horizontal gridlines and the value axis
  for (let i = 0; i <= 4; i++) {
    const value = (maxValue / 4) * i;
    const at = y(value);
    root.append(
      svg('line', { class: 'gridline', x1: pad.left, x2: width - pad.right, y1: at, y2: at }),
    );
    const label = svg('text', { class: 'tick', x: pad.left - 8, y: at + 3.5, 'text-anchor': 'end' });
    label.textContent = number(value);
    root.append(label);
  }

  // the time axis, a few ticks, never one per point
  root.append(
    svg('line', {
      class: 'axis',
      x1: pad.left,
      x2: width - pad.right,
      y1: height - pad.bottom,
      y2: height - pad.bottom,
    }),
  );
  for (let i = 0; i <= 3; i++) {
    const at = minAt + ((maxAt - minAt) / 3) * i;
    const label = svg('text', {
      class: 'tick',
      x: x(at),
      y: height - pad.bottom + 15,
      'text-anchor': i === 0 ? 'start' : i === 3 ? 'end' : 'middle',
    });
    label.textContent = clock(at);
    root.append(label);
  }

  for (const serie of series) {
    const color = `var(--series-${serie.slot + 1})`;
    const points = serie.points;
    const path = points.map((point, i) => `${i ? 'L' : 'M'}${x(point.at)},${y(point.value)}`).join(' ');
    root.append(svg('path', { class: 'serie', d: path, stroke: color }));

    if (points.length <= 30) {
      for (const point of points) {
        root.append(
          svg('circle', { class: 'point', cx: x(point.at), cy: y(point.value), r: 4, fill: color }),
        );
      }
    }

    // <= 4 series are labelled at the line end, identity is never colour alone
    if (series.length <= 4) {
      const last = points[points.length - 1];
      const label = svg('text', { class: 'tick', x: x(last.at) + 8, y: y(last.value) + 3.5 });
      label.textContent = serie.name;
      root.append(label);
    }
  }

  const crosshair = svg('line', {
    class: 'crosshair',
    y1: pad.top,
    y2: height - pad.bottom,
    x1: 0,
    x2: 0,
    opacity: 0,
  });
  root.append(crosshair);

  const tooltip = el('div', { class: 'tooltip' });
  tooltip.hidden = true;

  root.addEventListener('mousemove', (event) => {
    const box = root.getBoundingClientRect();
    const at = minAt + ((event.clientX - box.left) / box.width) * (maxAt - minAt);

    crosshair.setAttribute('opacity', 1);
    crosshair.setAttribute('x1', x(at));
    crosshair.setAttribute('x2', x(at));

    const rows = series
      .map((serie) => {
        const nearest = serie.points.reduce((best, point) =>
          Math.abs(point.at - at) < Math.abs(best.at - at) ? point : best,
        );
        return { serie, nearest };
      })
      .sort((a, b) => b.nearest.value - a.nearest.value);

    tooltip.replaceChildren(
      el('div', { class: 't-title', text: clock(rows[0].nearest.at) }),
      ...rows.map(({ serie, nearest }) =>
        el(
          'div',
          { class: 't-row' },
          el(
            'span',
            {},
            el('i', {
              class: 'swatch',
              style: `background: var(--series-${serie.slot + 1}); display:inline-block; margin-right:6px`,
            }),
            serie.name,
          ),
          el('span', { class: 'v', text: number(nearest.value) }),
        ),
      ),
    );

    tooltip.hidden = false;
    const left = Math.min(event.clientX - box.left + 14, box.width - 170);
    tooltip.style.left = `${Math.max(0, left)}px`;
    tooltip.style.top = `${Math.max(0, event.clientY - box.top - 10)}px`;
  });

  root.addEventListener('mouseleave', () => {
    crosshair.setAttribute('opacity', 0);
    tooltip.hidden = true;
  });

  host.replaceChildren(root, tooltip);
}

function niceMax(value) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  return Math.ceil(value / magnitude) * magnitude;
}

function renderLegend() {
  const legend = $('#chart-legend');
  const all = [...state.series.values()];
  const shown = all.filter((serie) => serie.slot < SERIES_SLOTS);

  const items = shown.map((serie) =>
    el(
      'button',
      {
        class: 'legend-item',
        'aria-pressed': String(serie.on),
        onClick: () => {
          serie.on = !serie.on;
          renderChart();
        },
      },
      el('i', { class: 'swatch', style: `background: var(--series-${serie.slot + 1})` }),
      serie.name,
    ),
  );

  // ! replaceChildren writes a null out as the text "null", the spread has to be clean
  if (all.length > SERIES_SLOTS) {
    items.push(
      el('span', {
        class: 'help',
        text: `另有 ${all.length - SERIES_SLOTS} 条序列未绘制，颜色只有 ${SERIES_SLOTS} 个槽位。`,
      }),
    );
  }

  legend.replaceChildren(...items);
}

function renderChartTable() {
  const series = visibleSeries().filter((serie) => serie.on && serie.points.length);
  const rows = series
    .flatMap((serie) => serie.points.map((point) => ({ name: serie.name, ...point })))
    .sort((a, b) => b.at - a.at)
    .slice(0, 100);

  $('#chart-table').replaceChildren(
    rows.length
      ? el(
          'table',
          { class: 'data' },
          el(
            'thead',
            {},
            el('tr', {}, el('th', { text: '时间' }), el('th', { text: '序列' }), el('th', { text: '值' })),
          ),
          el(
            'tbody',
            {},
            rows.map((row) =>
              el(
                'tr',
                {},
                el('td', { text: clock(row.at) }),
                el('td', { text: row.name }),
                el('td', { text: number(row.value) }),
              ),
            ),
          ),
        )
      : el('p', { class: 'empty', text: '还没有指标。' }),
  );
}
