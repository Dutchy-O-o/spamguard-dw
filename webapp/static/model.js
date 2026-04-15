// Model transparency page

(async () => {
  const r = await fetch("/api/model-metrics");
  if (!r.ok) {
    document.getElementById("model-loading").textContent = "Model could not be loaded.";
    return;
  }
  const j = await r.json();

  document.getElementById("model-loading").classList.add("hidden");
  document.getElementById("model-panel").classList.remove("hidden");

  // KPIs
  document.getElementById("m-acc").textContent  = (j.report.accuracy * 100).toFixed(1) + "%";
  document.getElementById("m-f1s").textContent  = j.report.spam["f1-score"].toFixed(3);
  document.getElementById("m-auc").textContent  = j.roc_curve.auc.toFixed(3);
  document.getElementById("m-nf").textContent   = fmt(j.n_features);
  document.getElementById("m-auc2").textContent = j.roc_curve.auc.toFixed(3);

  // Report table
  const rep = j.report;
  const tbody = document.querySelector("#m-report tbody");
  tbody.innerHTML = ["ham","spam","macro avg","weighted avg"].map(k => {
    const row = rep[k]; if (!row) return "";
    return `<tr>
      <td><b>${k}</b></td>
      <td>${row.precision.toFixed(3)}</td>
      <td>${row.recall.toFixed(3)}</td>
      <td>${row["f1-score"].toFixed(3)}</td>
      <td>${fmt(row.support)}</td>
    </tr>`;
  }).join("");

  // Confusion matrix
  const [[tn,fp],[fn,tp]] = j.confusion_matrix;
  document.getElementById("cm-tn").textContent = fmt(tn);
  document.getElementById("cm-fp").textContent = fmt(fp);
  document.getElementById("cm-fn").textContent = fmt(fn);
  document.getElementById("cm-tp").textContent = fmt(tp);

  // ROC
  drawCurve("roc-canvas", j.roc_curve.fpr, j.roc_curve.tpr, "FPR", "TPR", "#3b82f6", true);
  // PR
  drawCurve("pr-canvas", j.pr_curve.recall, j.pr_curve.precision, "Recall", "Precision", "#8b5cf6", false);

  // Top features
  document.getElementById("feat-spam").innerHTML = j.top_spam_features.map(f =>
    `<div class="feat"><b>${f.word}</b><span class="w-spam">+${f.weight.toFixed(2)}</span></div>`
  ).join("");
  document.getElementById("feat-ham").innerHTML = j.top_ham_features.map(f =>
    `<div class="feat"><b>${f.word}</b><span class="w-ham">${f.weight.toFixed(2)}</span></div>`
  ).join("");

  // Word cloud from /api/wordcloud
  const wc = await (await fetch("/api/wordcloud")).json();
  document.getElementById("wc-spam").innerHTML = wc.spam.map(w =>
    `<span class="wc-term spam" style="font-size:${w.size}px">${w.word}</span>`
  ).join(" ");
  document.getElementById("wc-ham").innerHTML = wc.ham.map(w =>
    `<span class="wc-term ham" style="font-size:${w.size}px">${w.word}</span>`
  ).join(" ");
})();

function drawCurve(id, xs, ys, xlabel, ylabel, color, diagonal) {
  const c = document.getElementById(id);
  if (!c) return;
  const ctx = c.getContext("2d");
  const W = c.width, H = c.height, pad = 40;
  ctx.clearRect(0,0,W,H);

  // axes
  ctx.strokeStyle = "rgba(255,255,255,0.1)";
  ctx.beginPath(); ctx.moveTo(pad, pad); ctx.lineTo(pad, H-pad); ctx.lineTo(W-pad, H-pad); ctx.stroke();

  // labels
  ctx.fillStyle = "#94a3b8"; ctx.font = "10px Inter, sans-serif";
  ctx.textAlign = "center"; ctx.fillText(xlabel, W/2, H-6);
  ctx.save(); ctx.translate(10, H/2); ctx.rotate(-Math.PI/2); ctx.fillText(ylabel, 0, 0); ctx.restore();
  for (let v=0; v<=1; v+=0.25) {
    ctx.textAlign = "right";
    ctx.fillText(v.toFixed(2), pad-4, H - pad - v*(H-2*pad) + 3);
    ctx.textAlign = "center";
    ctx.fillText(v.toFixed(2), pad + v*(W-2*pad), H - pad + 14);
  }

  // diagonal baseline for ROC
  if (diagonal) {
    ctx.strokeStyle = "rgba(148,163,184,0.3)";
    ctx.setLineDash([4,4]); ctx.beginPath();
    ctx.moveTo(pad, H-pad); ctx.lineTo(W-pad, pad);
    ctx.stroke(); ctx.setLineDash([]);
  }

  // curve
  ctx.strokeStyle = color; ctx.lineWidth = 2.5; ctx.beginPath();
  xs.forEach((x,i) => {
    const px = pad + x * (W - 2*pad);
    const py = H - pad - ys[i] * (H - 2*pad);
    if (i===0) ctx.moveTo(px,py); else ctx.lineTo(px,py);
  });
  ctx.stroke();
}
