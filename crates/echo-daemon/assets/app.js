/* bark-echo management face (ticket #32) — the thin vanilla-JS consumer
   of the /v1 JSON API (ADR-0005: the page is the API's consumer). No
   framework, no build step. One EventSource on /v1/events carries every
   realtime change; the page never polls. The field names are the
   echo-ipc vocabulary verbatim (crates/echo-ipc/src/message.rs);
   layout and flow are the browser-verified prototype's
   (`prototype/web-management-page`). */
"use strict";
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const subs = new Map(), logs = new Map(), unread = new Map(); // id -> line / entries / unseen count
// Device Keys this page's own add forms heard (AddedSubscription.device_key),
// keyed by id — /v1/status never serves the key (the snapshot carries public
// identity only), and a resync/reload forgets what no add answered here.
const pushKeys = new Map();
let selected = null, es = null, toastTimer = 0;

/* Every fetch carries the X-Requested-With custom header — the CSRF
   layer the state-change gate demands of the page (a cross-site fetch
   cannot pass preflight with it); SameSite=Strict keeps the cookie home
   regardless. */
async function api(path, opts = {}) {
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json", "X-Requested-With": "bark-echo-face" },
    body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
  });
  if (res.status === 401) { // session gone (expiry, rotate): back to the gate
    es?.close();
    $("#gate").hidden = false;
    throw Object.assign(new Error("unauthorized"), { status: 401 });
  }
  const data = await res.json().catch(() => null);
  if (!res.ok) throw Object.assign(new Error(data?.error?.message || res.statusText), { status: res.status });
  return data;
}

/* The one moment the page holds the token: POST /v1/session swaps it
   for the HttpOnly SameSite=Strict cookie and the answer carries no
   token — from here on the cookie is the session (issue #24 point 3:
   the page's JS never stores the token). */
async function login(token) {
  try {
    await api("/v1/session", { method: "POST", body: { token } });
    return true;
  } catch (err) {
    if (err.status !== 401) toast(`Session exchange failed: ${err.message}`, true);
    return false;
  }
}

/* echo-ipc StreamState serde `kind` tags -> indicator word + class.
   `disconnected` reads "reconnecting" — the retry loop is running; only
   `token_invalid`/`failed` are down (spec story 14: down is not quiet). */
const WORDS = { running: ["running", "ok"], starting: ["starting", "warn"],
  disconnected: ["reconnecting", "warn"], token_invalid: ["down", "bad"], failed: ["down", "bad"] };
const word = (s) => WORDS[s?.kind] || ["unknown", "bad"];

function renderSubs() {
  const ul = $("#sub-list");
  if (!subs.size) { ul.innerHTML = '<li class="empty">No subscriptions — add one.</li>'; return; }
  ul.textContent = "";
  for (const [id, s] of subs) {
    const [w, cls] = word(s.state);
    const n = unread.get(id) || 0;
    const li = document.createElement("li");
    li.className = "sub" + (id === selected ? " sel" : "");
    li.innerHTML = `
      <button type="button" class="sub-main" data-id="${esc(id)}" title="${esc(s.state?.detail || w)}">
        <span class="dot ${cls}"></span>
        <span class="sub-name">${esc(s.label)}</span>
        <span class="state-pill ${cls}">${w}</span>
        ${n ? `<span class="unread">${n}</span>` : ""}
        <span class="sub-meta">${esc(s.server)} · token ${esc(s.token_hint)}</span>
      </button>
      <button type="button" class="sub-remove" data-id="${esc(id)}" title="Remove subscription" aria-label="Remove ${esc(s.label)}">×</button>`;
    ul.appendChild(li);
  }
}

/* One History Log line: the NotificationIntent vocabulary (echo-core's,
   carried 1:1 by the snapshot and the appends), so both render as one
   thing. */
function logRow(entry, isNew) {
  const li = document.createElement("li");
  li.className = "entry" + (isNew ? " new" : "");
  const time = new Date((entry.created_at || 0) * 1000).toLocaleTimeString();
  const action = entry.click_action;
  li.innerHTML = `
    <div class="e-head">
      <span class="e-title">${esc(entry.title || "(no title)")}</span>
      <span class="e-time">#${esc(entry.id)} · ${time}</span>
    </div>
    ${entry.subtitle ? `<div class="e-sub">${esc(entry.subtitle)}</div>` : ""}
    <div class="e-body">${esc(entry.body)}</div>
    ${action?.type === "open_url" ? `<div class="e-url">${esc(action.url)}</div>` : ""}
    ${action?.type === "copy" ? `<div class="e-copy">copy — ${esc(action.text)}</div>` : ""}`;
  return li;
}

function renderLog() {
  $("#log-sub").textContent = subs.get(selected)?.label || "select a subscription";
  const ul = $("#log-list");
  ul.textContent = "";
  const entries = logs.get(selected) || [];
  if (!entries.length) { ul.innerHTML = '<li class="empty">No History Log entries yet.</li>'; return; }
  for (const entry of [...entries].reverse()) ul.appendChild(logRow(entry, false)); // oldest last -> newest first
}

function renderPushSelect() {
  const sel = $("#push-sub"), current = sel.value;
  sel.textContent = "";
  for (const [id, s] of subs) {
    const opt = document.createElement("option");
    opt.value = id;
    opt.textContent = s.label;
    sel.appendChild(opt);
  }
  if (subs.has(current)) sel.value = current;
  prefillDeviceKey();
}

/* The composer's Device Key is the send's explicit target (SendPush's
   own field) — prefill it with the key an add on this page answered;
   subscriptions added by another client leave the field to the user. */
function prefillDeviceKey() {
  const key = pushKeys.get($("#push-sub").value);
  if (key) $("#push-form [name=device_key]").value = key;
}

function toast(msg, bad) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast show" + (bad ? " bad" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.className = "toast"; }, 3500);
}

async function loadLog(id) {
  const data = await api(`/v1/log/${encodeURIComponent(id)}?tail=100`); // entries oldest last
  logs.set(id, data.entries);
  unread.set(id, 0);
}

async function refreshAll() {
  const snap = await api("/v1/status"); // StatusSnapshot verbatim: subscriptions keyed by id
  for (const id of subs.keys()) if (!snap.subscriptions[id]) { logs.delete(id); unread.delete(id); }
  subs.clear();
  for (const [id, line] of Object.entries(snap.subscriptions || {})) subs.set(id, line);
  renderSubs();
  renderPushSelect();
  if (!selected || !subs.has(selected)) selected = subs.keys().next().value || null;
  if (selected) await loadLog(selected);
  renderLog();
}

/* The one SSE stream: every realtime change rides it, serialized
   exactly as the channel's event frames (type-tagged echo-ipc Events). */
function connectEvents() {
  es = new EventSource("/v1/events"); // rides the session cookie
  es.onopen = () => setConn("live", "ok");
  es.onerror = () => setConn("reconnecting", "warn"); // EventSource retries on its own
  es.onmessage = (msg) => {
    let e;
    try { e = JSON.parse(msg.data); } catch { return; }
    if (e.type === "stream_state") {
      if (!subs.has(e.subscription_id)) { refreshAll().catch(() => {}); return; } // line grows in place
      const sub = subs.get(e.subscription_id);
      sub.label = e.label;
      sub.state = e.state;
      renderSubs();
    } else if (e.type === "history_appended") {
      logs.get(e.subscription_id)?.push(e.entry);
      if (e.subscription_id === selected && es?.readyState === 1) {
        $("#log-list")?.querySelector(".empty")?.remove();
        $("#log-list").prepend(logRow(e.entry, true));
      } else {
        unread.set(e.subscription_id, (unread.get(e.subscription_id) || 0) + 1);
        renderSubs();
      }
    } else if (e.type === "subscription_removed") {
      subs.delete(e.subscription_id);
      logs.delete(e.subscription_id);
      unread.delete(e.subscription_id);
      pushKeys.delete(e.subscription_id);
      if (selected === e.subscription_id) {
        selected = subs.keys().next().value || null;
        if (selected) loadLog(selected).then(renderLog).catch(() => {});
      }
      renderSubs();
      renderPushSelect();
      renderLog();
    } else if (e.type === "resync") refreshAll().catch(() => {}); // the hub lagged us: refetch
  };
}
const setConn = (w, cls) => { const c = $("#conn"); c.textContent = w; c.className = "conn " + cls; };

/* --- wiring --- */
$("#gate-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  if (await login($("#gate-token").value)) {
    $("#gate").hidden = true;
    $("#gate-token").value = "";
    $("#gate-error").hidden = true;
    try { await refreshAll(); connectEvents(); } catch {}
  } else {
    $("#gate-error").textContent = "Token rejected — check it and try again.";
    $("#gate-error").hidden = false;
  }
});

$("#add-open").addEventListener("click", () => { $("#add-form").hidden = !$("#add-form").hidden; });
$("#add-cancel").addEventListener("click", () => { $("#add-form").hidden = true; });

/* The Device source select maps onto AddSource's `kind` tags: `own`
   registers a fresh Device, `device_key` mirrors an existing one. */
const deviceKeyRow = $("#device-key-row");
$("#add-form [name=source_kind]").addEventListener("change", (ev) => {
  deviceKeyRow.hidden = ev.target.value !== "device_key";
});

/* The add answers once the supervisor is spawned: insert the line at
   `starting`; the flip to running arrives over the stream, like every
   other live change. */
$("#add-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  const source = f.get("source_kind") === "device_key"
    ? { kind: "device_key", device_key: (f.get("device_key") || "").trim() }
    : { kind: "own" };
  // An empty field means "not given" (BasicAuthCredentials's wire rule,
  // the same one the CLI flags build) — either half alone is valid.
  const user = f.get("auth_user") || "", password = f.get("auth_password") || "";
  const basic_auth = (user || password) ? { user, password } : undefined;
  try {
    const added = await api("/v1/subscriptions", { method: "POST", body: {
      server: (f.get("server") || "").trim(), source,
      label: (f.get("label") || "").trim(), basic_auth,
    } });
    subs.set(added.id, { label: added.label, server: added.server,
      token_hint: added.token_hint, state: { kind: "starting" } });
    pushKeys.set(added.id, added.device_key);
    renderSubs();
    renderPushSelect();
    $("#add-form").hidden = true;
    ev.target.reset();
    deviceKeyRow.hidden = true;
    toast(`Added — watch the state flip to running (Device Key ${added.device_key})`);
  } catch (err) {
    if (err.status !== 401) toast(`Add refused: ${err.message}`, true);
  }
});

$("#sub-list").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button");
  if (!btn) return;
  const id = btn.dataset.id;
  if (btn.classList.contains("sub-main")) {
    selected = id;
    unread.set(id, 0);
    try { await loadLog(id); } catch { return; }
    renderSubs();
    renderLog();
  } else if (btn.classList.contains("sub-remove") && confirm("Remove this subscription? The stream stops and its secrets are erased.")) {
    try { await api(`/v1/subscriptions/${encodeURIComponent(id)}`, { method: "DELETE" }); }
    catch (err) { if (err.status !== 401) toast(`Remove refused: ${err.message}`, true); }
    // the row drops via the subscription_removed event, like a second tab
  }
});

$("#push-sub").addEventListener("change", prefillDeviceKey);

$("#ext-add").addEventListener("click", () => {
  const row = $("#ext-rows").lastElementChild.cloneNode(true);
  for (const input of row.querySelectorAll("input")) input.value = "";
  $("#ext-rows").appendChild(row);
});

/* The send is SendPush verbatim: the subscription names the server (and
   the daemon's stored credentials), the Device Key is the explicit
   target. The outcome is the bark server's verdict — `sent` or
   `rejected` with the server's own code and message. */
$("#push-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target), ext_params = {};
  for (const row of document.querySelectorAll("#ext-rows .ext")) {
    const name = row.children[0].value.trim();
    if (name) ext_params[name] = row.children[1].value;
  }
  const subscription_id = f.get("subscription_id");
  try {
    const outcome = await api("/v1/push", { method: "POST", body: {
      subscription_id, device_key: f.get("device_key") || "",
      title: f.get("title") || null, subtitle: f.get("subtitle") || null,
      body: f.get("body") || null, ext_params,
    } });
    if (outcome.kind === "sent") {
      toast("Push sent — watch the History Log");
      selected = subscription_id;
      unread.set(subscription_id, 0);
      try { await loadLog(subscription_id); } catch {}
      renderSubs();
      renderLog();
      for (const el of ev.target.querySelectorAll("[name=title], [name=subtitle], [name=body]")) el.value = "";
    } else {
      toast(`Server refused: ${outcome.message} (code ${outcome.code ?? "?"})`, true);
    }
  } catch (err) {
    if (err.status !== 401) toast(`Send failed: ${err.message}`, true);
  }
});

/* Boot: a live cookie session goes straight in; anything else puts the
   gate up. The cookie is the only remembered credential — the token is
   never stored page-side (issue #24 point 3). */
(async () => {
  try { await refreshAll(); connectEvents(); return; } catch {}
  $("#gate").hidden = false;
})();
