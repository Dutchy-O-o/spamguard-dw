// Shared helpers across all pages

window.fmt = n => n === null || n === undefined ? "—" : Number(n).toLocaleString("en-US");

window.setModelPill = ok => {
  const el = document.getElementById("model-pill");
  if (!el) return;
  el.textContent = ok ? "model: ready" : "model: not loaded";
  el.classList.toggle("ok",  !!ok);
  el.classList.toggle("bad", !ok);
};

// Load model status on every page
fetch("/api/stats").then(r => r.json()).then(j => setModelPill(j.model_ready)).catch(() => setModelPill(false));

// ---- THEME toggle (init done in base.html <head> to avoid FOUC) ----
document.addEventListener("click", e => {
  if (!e.target.closest("#theme-toggle")) return;
  const h = document.documentElement;
  const next = (h.getAttribute("data-theme") === "dark") ? "light" : "dark";
  h.setAttribute("data-theme", next);
  localStorage.setItem("sg-theme", next);
});
