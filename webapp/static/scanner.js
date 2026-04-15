// Bulk CSV scanner

const dz     = document.getElementById("drop-zone");
const input  = document.getElementById("file-input");
const status = document.getElementById("scan-status");
const results = document.getElementById("scan-results");

let lastScored = null;   // cache for CSV download
let lastResult = null;   // cache for PDF export

dz.addEventListener("click", () => input.click());
dz.addEventListener("dragover", e => { e.preventDefault(); dz.classList.add("drag-over"); });
dz.addEventListener("dragleave", () => dz.classList.remove("drag-over"));
dz.addEventListener("drop", e => {
  e.preventDefault();
  dz.classList.remove("drag-over");
  if (e.dataTransfer.files.length) runScan(e.dataTransfer.files[0]);
});
input.addEventListener("change", () => {
  if (input.files.length) runScan(input.files[0]);
});

async function runScan(file) {
  if (!file.name.toLowerCase().endsWith(".csv")) {
    status.innerHTML = `<span style="color:var(--red)">CSV file expected.</span>`;
    return;
  }
  status.textContent = `Uploading ${file.name} (${(file.size/1024).toFixed(1)} KB)...`;

  const fd = new FormData();
  fd.append("csv", file);
  try {
    const r = await fetch("/api/scan", { method: "POST", body: fd });
    const j = await r.json();
    if (!r.ok) { status.innerHTML = `<span style="color:var(--red)">${j.error}</span>`; return; }
    status.innerHTML = `<span style="color:var(--green)">Done — ${j.total} rows scored.</span>`;
    lastScored = { name: file.name, rows: j.all_scored };
    lastResult = j;
    renderResults(j);
  } catch (e) {
    status.innerHTML = `<span style="color:var(--red)">${e.message}</span>`;
  }
}

function renderResults(j) {
  results.classList.remove("hidden");
  document.getElementById("sk-total").textContent = fmt(j.total);
  document.getElementById("sk-spam").textContent  = fmt(j.spam);
  document.getElementById("sk-ham").textContent   = fmt(j.ham);
  document.getElementById("sk-rate").textContent  = j.spam_rate_pct + "%";

  const max = Math.max(...j.distribution.counts);
  document.getElementById("dist-bars").innerHTML = j.distribution.labels.map((lbl, i) => {
    const n = j.distribution.counts[i];
    const pct = max ? (n / max * 100).toFixed(1) : 0;
    return `
      <div class="bar-row">
        <div class="muted">${lbl} – ${(parseFloat(lbl)+0.1).toFixed(1)}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        <div style="text-align:right">${fmt(n)}</div>
      </div>`;
  }).join("");

  document.querySelector("#tbl-risky tbody").innerHTML = j.top_risky.map(r => {
    const pct = (r.spam_probability * 100).toFixed(1);
    const color = r.is_spam ? "var(--red)" : "var(--green)";
    return `
      <tr>
        <td>${r.idx}</td>
        <td>${escapeHtml(r.subject) || '<span class="muted">—</span>'}</td>
        <td class="muted">${escapeHtml(r.body_preview)}</td>
        <td style="color:${color};font-weight:600">${pct}%</td>
      </tr>`;
  }).join("");

  window.scrollTo({ top: results.offsetTop - 60, behavior: "smooth" });
}

function escapeHtml(s) {
  return (s || "").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}[c]));
}

// ---- scan history ----
(async () => {
  try {
    const r = await fetch("/api/scan-history");
    const j = await r.json();
    const tbody = document.querySelector("#tbl-scan-history tbody");
    if (!j.history.length) { tbody.innerHTML = `<tr><td colspan="7" class="muted">no scans yet</td></tr>`; return; }
    tbody.innerHTML = j.history.map(h => `
      <tr>
        <td>${h.scan_id}</td>
        <td>${h.filename}</td>
        <td>${fmt(h.total)}</td>
        <td class="num-red">${fmt(h.spam)}</td>
        <td class="num-green">${fmt(h.ham)}</td>
        <td>${h.spam_rate}%</td>
        <td class="muted">${h.created_at}</td>
      </tr>`).join("");
  } catch {}
})();

// CSV download
// Defends against CSV Formula Injection (a.k.a. CSV Injection / CWE-1236):
// when Excel/LibreOffice opens a CSV, any cell starting with =, +, -, @, TAB
// or CR is interpreted as a formula. An attacker-controlled email subject like
// `=cmd|'/c calc'!A1` would execute. OWASP fix: prefix such cells with a
// single quote so the spreadsheet treats them as literal text.
function sanitizeForCsv(value) {
  let s = String(value ?? "");
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
  s = s.replace(/"/g, '""');
  return /[",\n]/.test(s) ? `"${s}"` : s;
}

document.getElementById("btn-download").addEventListener("click", () => {
  if (!lastScored) return;
  const header = ["idx","subject","body_preview","spam_probability","is_spam"];
  const lines = [header.join(",")];
  for (const r of lastScored.rows) {
    lines.push(header.map(h => sanitizeForCsv(r[h])).join(","));
  }
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = lastScored.name.replace(/\.csv$/i, "") + "_scored.csv";
  a.click();
});

// PDF download
document.getElementById("btn-pdf").addEventListener("click", async () => {
  if (!lastResult) return;
  const btn = document.getElementById("btn-pdf");
  const orig = btn.textContent;
  btn.textContent = "Generating report...";
  btn.disabled = true;
  try {
    const r = await fetch("/api/report", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(lastResult),
    });
    if (!r.ok) { alert("Report error: HTTP " + r.status); return; }
    const blob = await r.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "SpamGuard_Report.pdf";
    a.click();
  } catch (e) {
    alert("Error: " + e.message);
  } finally {
    btn.textContent = orig;
    btn.disabled = false;
  }
});
