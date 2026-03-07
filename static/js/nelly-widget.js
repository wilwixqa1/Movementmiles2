/* Movement & Miles — nelly-widget.js — Nelly chat widget */

var NELLY_API = '/api/chat';
var nellyHistory = [];

var pageMap = {
  'training programs': 'training',
  'training': 'training',
  'race plans': 'race',
  'race': 'race',
  'store': 'store',
  'home': 'home'
};

function toggleNelly() {
  document.getElementById('nelly-panel').classList.toggle('open');
  document.querySelector('.bubble-ping').style.display = 'none';
}

function renderBotText(text, container) {
  var parts = text.split(/\[\[page:([^\]]+)\]\]/g);
  for (var i = 0; i < parts.length; i++) {
    if (i % 2 === 0) {
      if (parts[i]) container.appendChild(document.createTextNode(parts[i]));
    } else {
      var pageName = parts[i];
      var pageKey = pageMap[pageName.toLowerCase()] || 'home';
      var a = document.createElement('a');
      a.textContent = pageName;
      a.href = '#';
      a.className = 'nelly-link';
      a.setAttribute('data-page', pageKey);
      a.onclick = function(e) {
        e.preventDefault();
        var pg = this.getAttribute('data-page');
        toggleNelly();
        showPage(pg);
      };
      container.appendChild(a);
    }
  }
}

function parseButtons(text) {
  var match = text.match(/\[([^\]]+)\]\s*$/);
  if (!match) return { text: text, buttons: [] };
  var cleanText = text.replace(/\[([^\]]+)\]\s*$/, '').trim();
  var inner = match[1];
  if (inner.indexOf('|') === -1) return { text: text, buttons: [] };
  var buttons = inner.split('|').map(function(b) { return b.trim(); });
  return { text: cleanText, buttons: buttons };
}

function addBotMessage(text) {
  var msgs = document.getElementById('nelly-msgs');
  var parsed = parseButtons(text);
  var b = document.createElement('div');
  b.className = 'nmsg bot';
  renderBotText(parsed.text, b);
  msgs.appendChild(b);
  if (parsed.buttons.length > 0) {
    var btnRow = document.createElement('div');
    btnRow.className = 'nelly-buttons';
    parsed.buttons.forEach(function(label) {
      var btn = document.createElement('button');
      btn.className = 'nelly-qr';
      btn.textContent = label;
      btn.onclick = function() {
        btnRow.remove();
        sendMsgText(label);
      };
      btnRow.appendChild(btn);
    });
    msgs.appendChild(btnRow);
  }
  msgs.scrollTop = msgs.scrollHeight;
}

function sendMsgText(text) {
  var msgs = document.getElementById('nelly-msgs');
  var typing = document.getElementById('nelly-typing');
  var u = document.createElement('div');
  u.className = 'nmsg user';
  u.textContent = text;
  msgs.appendChild(u);
  msgs.scrollTop = msgs.scrollHeight;
  nellyHistory.push({ role: 'user', content: text });
  typing.classList.add('show');
  msgs.scrollTop = msgs.scrollHeight;
  fetch(NELLY_API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: text, history: nellyHistory.slice(0, -1) })
  }).then(function(res) {
    typing.classList.remove('show');
    if (!res.ok) throw new Error('Server error: ' + res.status);
    return res.json();
  }).then(function(data) {
    var reply = data.reply || "Sorry, couldn't connect right now. Try again!";
    nellyHistory.push({ role: 'assistant', content: reply });
    addBotMessage(reply);
  }).catch(function(err) {
    typing.classList.remove('show');
    var b = document.createElement('div');
    b.className = 'nmsg bot';
    b.textContent = 'Having trouble connecting right now. Try again in a moment!';
    msgs.appendChild(b);
    msgs.scrollTop = msgs.scrollHeight;
    console.error('Nelly error:', err);
  });
}

function sendMsg() {
  var input = document.getElementById('nelly-input');
  var text = input.value.trim();
  if (!text) return;
  input.value = '';
  sendMsgText(text);
}
