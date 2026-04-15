// AI Assistant — simple chat over DW

const chat  = document.getElementById("chat");
const input = document.getElementById("ask-input");
const btn   = document.getElementById("ask-btn");

function escapeHtml(s) {
  return (s || "").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}[c]));
}

function mdInline(s) {
  // basic: **bold**, `code`, \n -> <br>
  return escapeHtml(s)
    .replace(/\*\*(.+?)\*\*/g, "<b>$1</b>")
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\n/g, "<br>");
}

function addUser(text) {
  const el = document.createElement("div");
  el.className = "msg user";
  el.textContent = text;
  chat.appendChild(el);
  chat.scrollTop = chat.scrollHeight;
}

function addBot(payload) {
  const el = document.createElement("div");
  el.className = "msg bot";
  let html = `<pre>${mdInline(payload.text || "")}</pre>`;
  if (payload.table) {
    html += `<table><thead><tr>${payload.table.columns.map(c => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
    html += `<tbody>${payload.table.rows.map(row =>
      `<tr>${row.map(c => `<td>${escapeHtml(String(c))}</td>`).join("")}</tr>`
    ).join("")}</tbody></table>`;
  }
  el.innerHTML = html;
  chat.appendChild(el);
  chat.scrollTop = chat.scrollHeight;
}

function addBotLoading() {
  const el = document.createElement("div");
  el.className = "msg bot";
  el.id = "tmp-loading";
  el.innerHTML = `<span class="muted">thinking...</span>`;
  chat.appendChild(el);
  chat.scrollTop = chat.scrollHeight;
}

async function ask(q) {
  if (!q.trim()) return;
  addUser(q);
  input.value = "";
  addBotLoading();

  try {
    const r = await fetch("/api/ask", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: q }),
    });
    const j = await r.json();
    document.getElementById("tmp-loading")?.remove();
    addBot(j);
  } catch (e) {
    document.getElementById("tmp-loading")?.remove();
    addBot({ text: "Error: " + e.message });
  }
}

btn.addEventListener("click", () => ask(input.value));
input.addEventListener("keydown", e => { if (e.key === "Enter") ask(input.value); });

document.querySelectorAll(".chip").forEach(c => {
  c.addEventListener("click", () => ask(c.textContent));
});

// opening
addBot({ text: "Hi — ask me anything about the 517K emails in the data warehouse. Click a suggestion above to get started." });
