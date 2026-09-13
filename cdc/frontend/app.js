// Vanilla JS, no build step. Every fetch() below uses a root-relative path
// (leading "/") deliberately -- this page is served at /app/, but the API
// routes live at the origin root, and a leading slash resolves against the
// origin regardless of the page's own path. Same-origin throughout (see
// cdc/api/main.py's docstring), so no CORS handling needed anywhere here.

const CHART_COLORS = [
  "#3b5bdb", "#e8590c", "#2f9e44", "#ae3ec9", "#f08c00", "#1098ad", "#e03131",
];

async function api(path) {
  const res = await fetch(path);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `${res.status} ${res.statusText}`);
  }
  return res.json();
}

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else node.setAttribute(k, v);
  }
  for (const child of children) {
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
}

function renderTable(container, columns, rows, rowClassFn) {
  container.innerHTML = "";
  if (!rows.length) {
    container.appendChild(el("p", { class: "empty" }, ["No rows."]));
    return;
  }
  const thead = el("thead", {}, [el("tr", {}, columns.map((c) => el("th", {}, [c.label])))]);
  const tbody = el(
    "tbody",
    {},
    rows.map((row) => {
      const cls = rowClassFn ? rowClassFn(row) : "";
      return el(
        "tr",
        cls ? { class: cls } : {},
        columns.map((c) => el("td", {}, [String(c.fmt ? c.fmt(row[c.key]) : row[c.key] ?? "")]))
      );
    })
  );
  container.appendChild(el("table", {}, [thead, tbody]));
}

function safeChart(existing, canvasId, config) {
  // A chart failing to render (CDN hiccup, ad-blocker, a bad version pin --
  // exactly what happened here once already) must not take the rest of the
  // section down with it. Without this, `new Chart(...)` throwing aborts
  // the calling function immediately, so the table render call right after
  // it never runs either -- the whole section goes silently blank, chart
  // and table both, with nothing but a console error to explain why.
  if (existing) existing.destroy();
  try {
    return new Chart(document.getElementById(canvasId), config);
  } catch (e) {
    console.error(`Chart render failed for #${canvasId}:`, e);
    return null;
  }
}

function fillSelect(select, values, currentPlaceholderKept = true) {
  const existing = new Set(Array.from(select.options).map((o) => o.value));
  for (const v of values) {
    if (!existing.has(v)) select.appendChild(el("option", { value: v }, [v]));
  }
}

// ---- health ----
async function checkHealth() {
  const line = document.getElementById("health-line");
  try {
    const data = await api("/health");
    if (data.status === "ok") {
      line.textContent = `API OK — marts available: ${data.mart_tables.join(", ")}`;
      line.className = "health ok";
    } else {
      line.textContent = `API not ready: ${data.detail}`;
      line.className = "health error";
    }
  } catch (e) {
    line.textContent = `API unreachable: ${e.message}`;
    line.className = "health error";
  }
}

// ---- revenue by channel ----
let revenueChart = null;

async function loadRevenue() {
  const channel = document.getElementById("revenue-channel").value;
  const start = document.getElementById("revenue-start").value;
  const end = document.getElementById("revenue-end").value;

  const params = new URLSearchParams();
  if (channel) params.set("channel", channel);
  if (start) params.set("start_date", start);
  if (end) params.set("end_date", end);

  const rows = await api(`/metrics/revenue-by-channel?${params}`);

  fillSelect(document.getElementById("revenue-channel"), [...new Set(rows.map((r) => r.channel))].sort());

  const dates = [...new Set(rows.map((r) => r.order_date.slice(0, 10)))].sort();
  const channels = [...new Set(rows.map((r) => r.channel))].sort();
  const byChannelDate = {};
  for (const r of rows) byChannelDate[`${r.channel}|${r.order_date.slice(0, 10)}`] = r.total_revenue;

  const datasets = channels.map((ch, i) => ({
    label: ch,
    data: dates.map((d) => byChannelDate[`${ch}|${d}`] ?? 0),
    borderColor: CHART_COLORS[i % CHART_COLORS.length],
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
    tension: 0.2,
  }));

  revenueChart = safeChart(revenueChart, "revenue-chart", {
    type: "line",
    data: { labels: dates, datasets },
    options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } } },
  });

  renderTable(
    document.getElementById("revenue-table"),
    [
      { key: "order_date", label: "Date", fmt: (v) => v.slice(0, 10) },
      { key: "channel", label: "Channel" },
      { key: "order_count", label: "Orders" },
      { key: "total_revenue", label: "Revenue", fmt: (v) => `$${v.toFixed(2)}` },
      { key: "avg_order_value", label: "Avg order" },
    ],
    rows
  );
}

// ---- customers by region ----
let customersChart = null;

async function loadCustomers() {
  const rows = await api("/metrics/customers-by-region");
  const regions = [...new Set(rows.map((r) => r.region))].sort();
  const channels = [...new Set(rows.map((r) => r.acquisition_channel))].sort();
  const byRegionChannel = {};
  for (const r of rows) byRegionChannel[`${r.region}|${r.acquisition_channel}`] = r.customer_count;

  const datasets = channels.map((ch, i) => ({
    label: ch,
    data: regions.map((rg) => byRegionChannel[`${rg}|${ch}`] ?? 0),
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
  }));

  customersChart = safeChart(customersChart, "customers-chart", {
    type: "bar",
    data: { labels: regions, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } },
    },
  });
}

// ---- inventory ----
async function loadInventory() {
  const warehouse = document.getElementById("inventory-warehouse").value;
  const reorderOnly = document.getElementById("inventory-reorder-only").checked;

  const params = new URLSearchParams();
  if (warehouse) params.set("warehouse", warehouse);
  if (reorderOnly) params.set("needs_reorder", "true");

  const rows = await api(`/metrics/inventory?${params}`);
  fillSelect(document.getElementById("inventory-warehouse"), [...new Set(rows.map((r) => r.warehouse))].sort());

  renderTable(
    document.getElementById("inventory-table"),
    [
      { key: "product_id", label: "Product" },
      { key: "warehouse", label: "Warehouse" },
      { key: "quantity_on_hand", label: "On hand" },
      { key: "quantity_reserved", label: "Reserved" },
      { key: "quantity_available", label: "Available" },
      { key: "reorder_point", label: "Reorder pt" },
      { key: "as_of_date", label: "As of", fmt: (v) => v.slice(0, 10) },
    ],
    rows,
    (row) => (row.needs_reorder ? "needs-reorder" : "")
  );
}

// ---- orders ----
const ORDERS_PAGE_SIZE = 20;
let ordersOffset = 0;

async function loadOrders() {
  const status = document.getElementById("orders-status").value;
  const channel = document.getElementById("orders-channel").value;

  const params = new URLSearchParams({ limit: String(ORDERS_PAGE_SIZE), offset: String(ordersOffset) });
  if (status) params.set("status", status);
  if (channel) params.set("channel", channel);

  const rows = await api(`/orders?${params}`);
  fillSelect(document.getElementById("orders-channel"), [...new Set(rows.map((r) => r.channel))].sort());

  renderTable(
    document.getElementById("orders-table"),
    [
      { key: "order_id", label: "Order" },
      { key: "order_date", label: "Date", fmt: (v) => v.slice(0, 10) },
      { key: "customer_id", label: "Customer" },
      { key: "customer_region", label: "Region" },
      { key: "status", label: "Status" },
      { key: "channel", label: "Channel" },
      { key: "order_revenue", label: "Revenue", fmt: (v) => `$${Number(v).toFixed(2)}` },
    ],
    rows
  );

  document.getElementById("orders-page-label").textContent = `Rows ${ordersOffset + 1}–${ordersOffset + rows.length}`;
  document.getElementById("orders-prev").disabled = ordersOffset === 0;
  document.getElementById("orders-next").disabled = rows.length < ORDERS_PAGE_SIZE;
}

// ---- wiring ----
document.getElementById("revenue-apply").addEventListener("click", loadRevenue);
document.getElementById("inventory-warehouse").addEventListener("change", loadInventory);
document.getElementById("inventory-reorder-only").addEventListener("change", loadInventory);
document.getElementById("orders-status").addEventListener("change", () => {
  ordersOffset = 0;
  loadOrders();
});
document.getElementById("orders-channel").addEventListener("change", () => {
  ordersOffset = 0;
  loadOrders();
});
document.getElementById("orders-prev").addEventListener("click", () => {
  ordersOffset = Math.max(0, ordersOffset - ORDERS_PAGE_SIZE);
  loadOrders();
});
document.getElementById("orders-next").addEventListener("click", () => {
  ordersOffset += ORDERS_PAGE_SIZE;
  loadOrders();
});

async function init() {
  await checkHealth();
  const results = await Promise.allSettled([loadRevenue(), loadCustomers(), loadInventory(), loadOrders()]);
  results.forEach((r) => {
    if (r.status === "rejected") console.error(r.reason);
  });
}

init();
