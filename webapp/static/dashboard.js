// Dashboard page — loads stats, tables, bars, runs live checker

async function fetchStats() {
  const r = await fetch("/api/stats");
  const j = await r.json();
  renderKpis(j.overview);
  renderDomains(j.top_domains);
  renderWeekday(j.by_weekday);
  renderSenders(j.top_senders);
}

function renderKpis(o) {
  const set = (id, val) => {
    const el = document.getElementById(id);
    el.classList.remove("loading");
    el.textContent = val;
  };
  set("kpi-total", fmt(o.total_emails));
  set("kpi-spam",  fmt(o.spam_count));
  set("kpi-ham",   fmt(o.ham_count));
  set("kpi-unlab", fmt(o.unlabeled_count));
  set("kpi-rate",  (o.spam_rate_pct ?? 0) + "%");
  set("kpi-words", fmt(o.avg_word_count));
}

function renderDomains(rows) {
  document.querySelector("#tbl-domains tbody").innerHTML = rows.map(r => `
    <tr class="clickable" data-type="domain" data-value="${r.domain}">
      <td><b>${r.domain}</b></td>
      <td>${fmt(r.total_emails)}</td>
      <td class="num-red">${fmt(r.spam_count)}</td>
      <td>${r.spam_rate_pct ?? "—"}${r.spam_rate_pct != null ? "%" : ""}</td>
      <td><span class="badge ${r.is_internal ? "int" : "ext"}">${r.is_internal ? "internal" : "external"}</span></td>
    </tr>`).join("");
}

function renderSenders(rows) {
  document.querySelector("#tbl-senders tbody").innerHTML = rows.map(r => `
    <tr class="clickable" data-type="sender" data-value="${r.email_address}">
      <td><b>${r.email_address}</b></td>
      <td>${r.domain}</td>
      <td>${fmt(r.total_emails)}</td>
      <td class="num-red">${fmt(r.spam_count)}</td>
      <td>${r.spam_rate_pct ?? "—"}${r.spam_rate_pct != null ? "%" : ""}</td>
    </tr>`).join("");
}

// ---- DRILL-DOWN ----
const modal = document.getElementById("dd-modal");
const ddBody = document.getElementById("dd-body");

document.addEventListener("click", async e => {
  const close = e.target.closest("[data-close]");
  if (close) { modal.classList.add("hidden"); return; }
  const row = e.target.closest("tr.clickable");
  if (!row) return;
  openDrilldown(row.dataset.type, row.dataset.value);
});

async function openDrilldown(type, value) {
  modal.classList.remove("hidden");
  ddBody.innerHTML = `<div class="muted">Loading...</div>`;
  try {
    const r = await fetch(`/api/drilldown?type=${type}&value=${encodeURIComponent(value)}`);
    const j = await r.json();
    if (!r.ok) { ddBody.innerHTML = `<div class="verdict danger">${j.error}</div>`; return; }
    ddBody.innerHTML = type === "domain" ? renderDomainDrill(j) : renderSenderDrill(j);
  } catch (e) {
    ddBody.innerHTML = `<div class="verdict danger">${e.message}</div>`;
  }
}

function renderDomainDrill(j) {
  const h = j.head;
  const wdMax = Math.max(...j.weekday.map(r => r.total));
  return `
    <div class="dd-header">
      <div class="dd-title">${h.domain}
        <span class="badge ${h.is_internal ? "int" : "ext"}" style="margin-left:8px">${h.is_internal ? "internal" : "external"}</span>
      </div>
      <div class="dd-kpis">
        <div><div class="muted">Total</div><b>${fmt(h.total_emails)}</b></div>
        <div><div class="muted">Spam</div><b class="num-red">${fmt(h.spam_count)}</b></div>
        <div><div class="muted">Ham</div><b class="num-green">${fmt(h.ham_count)}</b></div>
        <div><div class="muted">Spam Rate</div><b>${h.spam_rate_pct ?? 0}%</b></div>
      </div>
    </div>

    <h3>Top Senders in ${h.domain}</h3>
    <table class="data"><thead><tr><th>Sender</th><th>Emails</th><th>Spam</th></tr></thead>
      <tbody>
        ${j.top_senders.map(r => `<tr>
          <td>${r.email_address}</td>
          <td>${fmt(r.total)}</td>
          <td class="num-red">${fmt(r.spam || 0)}</td>
        </tr>`).join("")}
      </tbody>
    </table>

    <h3>Weekday distribution</h3>
    <div class="bars">
      ${j.weekday.map(r => `
        <div class="bar-row">
          <div class="muted">${r.day_name}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${(r.total/wdMax*100).toFixed(1)}%"></div></div>
          <div style="text-align:right">${fmt(r.total)} <span class="muted">· spam ${fmt(r.spam||0)}</span></div>
        </div>`).join("")}
    </div>`;
}

function renderSenderDrill(j) {
  const h = j.head;
  return `
    <div class="dd-header">
      <div class="dd-title">${h.email_address}
        <span class="badge ${h.is_internal ? "int" : "ext"}" style="margin-left:8px">${h.domain}</span>
      </div>
      <div class="dd-kpis">
        <div><div class="muted">Total</div><b>${fmt(h.total)}</b></div>
        <div><div class="muted">Spam</div><b class="num-red">${fmt(h.spam || 0)}</b></div>
        <div><div class="muted">Ham</div><b class="num-green">${fmt(h.ham || 0)}</b></div>
        <div><div class="muted">Avg words</div><b>${h.avg_words ?? "—"}</b></div>
        <div><div class="muted">Avg links</div><b>${h.avg_links ?? "—"}</b></div>
      </div>
    </div>

    <h3>Top Subjects</h3>
    <table class="data"><thead><tr><th>Subject</th><th>Count</th><th>Spam</th></tr></thead>
      <tbody>
        ${j.top_subjects.map(r => `<tr>
          <td>${(r.subject_text || "<i class='muted'>(empty)</i>").slice(0,90)}</td>
          <td>${fmt(r.n)}</td>
          <td class="num-red">${fmt(r.spam || 0)}</td>
        </tr>`).join("")}
      </tbody>
    </table>

    <h3>Monthly Timeline</h3>
    <div class="bars">
      ${(() => {
        const tmax = Math.max(...j.timeline.map(r => r.n));
        return j.timeline.map(r => `
          <div class="bar-row">
            <div class="muted">${r.year}-${String(r.month).padStart(2,"0")}</div>
            <div class="bar-track"><div class="bar-fill" style="width:${(r.n/tmax*100).toFixed(1)}%"></div></div>
            <div style="text-align:right">${fmt(r.n)} <span class="muted">· spam ${fmt(r.spam||0)}</span></div>
          </div>`).join("");
      })()}
    </div>`;
}

function renderWeekday(rows) {
  const max = Math.max(...rows.map(r => r.total_emails));
  document.getElementById("weekday-bars").innerHTML = rows.map(r => `
    <div class="bar-row">
      <div class="muted">${r.day_name}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${(r.total_emails / max * 100).toFixed(1)}%"></div></div>
      <div style="text-align:right">${fmt(r.total_emails)} <span class="muted">· ${r.spam_rate_pct ?? 0}%</span></div>
    </div>`).join("");
}

// Live checker
document.getElementById("btn-check").addEventListener("click", async () => {
  const subject = document.getElementById("in-subject").value;
  const body    = document.getElementById("in-body").value;
  const res = document.getElementById("result");
  res.classList.remove("empty");
  res.innerHTML = `<div class="muted">Checking...</div>`;

  try {
    const r = await fetch("/api/check", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ subject, body }),
    });
    const j = await r.json();
    if (!r.ok) { res.innerHTML = `<div class="verdict danger">Error</div><div class="muted">${j.error}</div>`; return; }

    const spamPct = (j.spam_probability * 100).toFixed(1);
    const hamPct  = (j.ham_probability  * 100).toFixed(1);
    const f = j.features;
    res.innerHTML = `
      <div class="verdict ${j.tone}">${j.verdict}</div>
      <div class="muted" style="margin-bottom:6px">Spam probability: <b style="color:var(--text)">${spamPct}%</b></div>
      <div class="prob-bar"><div class="prob-fill spam" style="width:${spamPct}%"></div></div>
      <div class="muted" style="font-size:11px">Ham: ${hamPct}% · Spam: ${spamPct}%</div>
      <div class="feats">
        <div>Word count:  <b>${f.word_count}</b></div>
        <div>Char count:  <b>${f.char_count}</b></div>
        <div>Link count:  <b>${f.link_count}</b></div>
        <div>Upper ratio: <b>${f.upper_ratio}</b></div>
        <div>Urgent kw:   <b>${f.has_urgent ? "yes" : "no"}</b></div>
        <div>Money kw:    <b>${f.has_money ? "yes" : "no"}</b></div>
      </div>`;
  } catch (e) {
    res.innerHTML = `<div class="verdict danger">Error</div><div class="muted">${e.message}</div>`;
  }
});

fetchStats();

// ---- TREND chart (canvas) ----
(async () => {
  const r = await fetch("/api/trend");
  const j = await r.json();
  drawTrend(j.points);
})();

function drawTrend(pts) {
  const c = document.getElementById("trend-canvas");
  if (!c) return;
  const ctx = c.getContext("2d");
  const W = c.width, H = c.height, pad = 40;
  ctx.clearRect(0,0,W,H);

  if (!pts.length) return;
  const rates = pts.map(p => p.spam_rate || 0);
  const totals = pts.map(p => p.total || 0);
  const maxRate = Math.max(...rates, 1);
  const maxTot  = Math.max(...totals, 1);

  // axes
  ctx.strokeStyle = "rgba(255,255,255,0.1)";
  ctx.beginPath(); ctx.moveTo(pad, pad); ctx.lineTo(pad, H-pad); ctx.lineTo(W-pad, H-pad); ctx.stroke();

  // bars (total) — soft
  const bw = (W - 2*pad) / pts.length;
  pts.forEach((p,i) => {
    const h = (p.total / maxTot) * (H - 2*pad);
    ctx.fillStyle = "rgba(139,92,246,0.18)";
    ctx.fillRect(pad + i*bw + 1, H - pad - h, bw - 2, h);
  });

  // line (spam rate)
  ctx.strokeStyle = "#ef4444"; ctx.lineWidth = 2; ctx.beginPath();
  pts.forEach((p,i) => {
    const x = pad + i*bw + bw/2;
    const y = H - pad - (p.spam_rate / maxRate) * (H - 2*pad);
    if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
  });
  ctx.stroke();

  // points + selective labels
  pts.forEach((p,i) => {
    const x = pad + i*bw + bw/2;
    const y = H - pad - (p.spam_rate / maxRate) * (H - 2*pad);
    ctx.fillStyle = "#ef4444"; ctx.beginPath(); ctx.arc(x,y,3,0,Math.PI*2); ctx.fill();
    if (i % Math.max(1, Math.floor(pts.length/12)) === 0) {
      ctx.fillStyle = "#94a3b8"; ctx.font = "10px Inter, sans-serif";
      ctx.textAlign = "center";
      ctx.fillText(`${p.year}-${String(p.month).padStart(2,"0")}`, x, H - pad + 14);
    }
  });

  // legend
  ctx.fillStyle = "#94a3b8"; ctx.font = "11px Inter, sans-serif"; ctx.textAlign = "left";
  ctx.fillText("▬ spam rate (%)   ▬ volume (bar)",  pad, 18);
  ctx.fillText(`max spam rate: ${maxRate.toFixed(1)}%`, W - pad - 150, 18);
}

// ---- ANOMALIES ----
(async () => {
  const r = await fetch("/api/anomalies");
  const j = await r.json();
  const el = document.getElementById("anomaly-list");
  if (!el) return;
  if (!j.anomalies.length) {
    el.innerHTML = `<div class="muted">No notable anomalies.</div>`; return;
  }
  el.innerHTML = j.anomalies.map(a => `
    <div class="anomaly ${a.level}">
      <div class="anomaly-icon">${a.icon}</div>
      <div>
        <div class="anomaly-title">${a.title}</div>
        <div class="anomaly-detail">${a.detail}</div>
      </div>
    </div>`).join("");
})();

// ---- EXPLAIN: fetch top contributing tokens after a prediction renders ----
const resultDiv = document.getElementById("result");
const obs = new MutationObserver(async () => {
  if (!resultDiv.querySelector(".verdict") || resultDiv.querySelector(".explain-box")) return;
  const subject = document.getElementById("in-subject").value;
  const body    = document.getElementById("in-body").value;
  try {
    const r = await fetch("/api/explain", {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ subject, body }),
    });
    const j = await r.json();
    if (!j.top_contributors || !j.top_contributors.length) return;
    const box = document.createElement("div");
    box.className = "explain-box";
    box.innerHTML = `<h4>Why? — top contributing tokens</h4>
      <div class="tokens">
        ${j.top_contributors.slice(0,10).map(t => `
          <span class="token ${t.weight > 0 ? 'spam' : 'ham'}">${t.word} ${t.weight > 0 ? '+' : ''}${t.weight.toFixed(2)}</span>
        `).join("")}
      </div>`;
    resultDiv.appendChild(box);
  } catch {}
});
obs.observe(resultDiv, { childList: true, subtree: true });
