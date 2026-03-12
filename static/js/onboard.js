/* Movement & Miles -- onboard.js -- Session 12 */
/* Uses /api/chat (same as widget). No lead capture. */

var GS_API = '/api/chat';
var gsHistory = [];

var STRIPE_CHECKOUT_URL = 'https://buy.stripe.com/test_cNiaEWajyfKq9xa47R0kE00';

function gsParseButtons(text) {
  var match = text.match(/\[([^\]]+)\]\s*$/);
  if (!match) return { text: text, buttons: [] };
  var cleanText = text.replace(/\[([^\]]+)\]\s*$/, '').trim();
  var inner = match[1];
  if (inner.indexOf('|') === -1) return { text: text, buttons: [] };
  var buttons = inner.split('|').map(function(b) { return b.trim(); });
  return { text: cleanText, buttons: buttons };
}

function gsParsePageLinks(text) {
  return text.replace(/\[\[page:([^\]]+)\]\]/g, function(m, pageName) {
    var slug = pageName.toLowerCase().replace(/\s+/g, '-');
    return '<a href="#' + slug + '" class="gs-page-link" onclick="if(window.showPage)window.showPage(\'' + slug + '\')">' + pageName + '</a>';
  });
}

function gsAddBotMessage(text) {
  var msgs = document.getElementById('gs-msgs');
  var parsed = gsParseButtons(text);
  var b = document.createElement('div');
  b.className = 'gs-msg bot';
  b.innerHTML = gsParsePageLinks(parsed.text);
  msgs.appendChild(b);
  if (parsed.buttons.length > 0) {
    var btnRow = document.createElement('div');
    btnRow.className = 'gs-buttons';
    parsed.buttons.forEach(function(label) {
      var btn = document.createElement('button');
      btn.className = 'gs-qr';
      btn.textContent = label;
      btn.onclick = function() {
        btnRow.remove();
        gsSendMsgText(label);
      };
      btnRow.appendChild(btn);
    });
    msgs.appendChild(btnRow);
  }
  msgs.scrollTop = msgs.scrollHeight;
}

function gsSendMsgText(text) {
  var msgs = document.getElementById('gs-msgs');
  var typing = document.getElementById('gs-typing');
  var u = document.createElement('div');
  u.className = 'gs-msg user';
  u.textContent = text;
  msgs.appendChild(u);
  msgs.scrollTop = msgs.scrollHeight;
  gsHistory.push({ role: 'user', content: text });
  typing.classList.add('show');
  msgs.scrollTop = msgs.scrollHeight;
  fetch(GS_API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: text, history: gsHistory.slice(0, -1), source: 'onboard' })
  }).then(function(res) {
    typing.classList.remove('show');
    if (!res.ok) throw new Error('Server error: ' + res.status);
    return res.json();
  }).then(function(data) {
    var reply = data.reply || "Sorry, couldn't connect right now. Try again!";
    gsHistory.push({ role: 'assistant', content: reply });
    gsAddBotMessage(reply);
  }).catch(function(err) {
    typing.classList.remove('show');
    var b = document.createElement('div');
    b.className = 'gs-msg bot';
    b.textContent = 'Having trouble connecting right now. Try again in a moment!';
    msgs.appendChild(b);
    msgs.scrollTop = msgs.scrollHeight;
    console.error('Chat error:', err);
  });
}

function gsSendMsg() {
  var input = document.getElementById('gs-input');
  var text = input.value.trim();
  if (!text) return;
  input.value = '';
  gsSendMsgText(text);
}