/* Movement & Miles — onboard.js — Get-started onboarding chat */

var GS_API = '/api/onboard-chat';
var gsHistory = [];

/* ── STRIPE CHECKOUT URL ──
   Replace this placeholder with your real Stripe Payment Link.
   Example: https://buy.stripe.com/your_link_id
   You can also use a Stripe Checkout Session URL from your backend.
*/
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

function gsStripLeadTag(text) {
  var match = text.match(/\[\[LEAD:(.*?)\]\]/);
  if (!match) return { text: text, lead: null };
  var cleanText = text.replace(/\[\[LEAD:.*?\]\]/, '').trim();
  try {
    var lead = JSON.parse(match[1]);
    return { text: cleanText, lead: lead };
  } catch (e) {
    console.error('Failed to parse lead JSON:', e);
    return { text: text, lead: null };
  }
}

function gsSaveLead(leadData) {
  // Attach UTM data from cookie (set on landing by index.html)
  try {
    var match = document.cookie.match(/mm_utm=([^;]+)/);
    if (match) {
      var utm = JSON.parse(decodeURIComponent(match[1]));
      leadData.utm_source = utm.s || '';
      leadData.utm_medium = utm.m || '';
      leadData.utm_campaign = utm.c || '';
    }
  } catch(e) { console.error('UTM read error:', e); }

  fetch('/api/lead', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(leadData)
  }).then(function(res) {
    if (!res.ok) console.error('Lead save failed:', res.status);
  }).catch(function(err) {
    console.error('Lead save error:', err);
  });
}

function gsShowThankYou() {
  var chat = document.querySelector('.gs-chat');
  chat.innerHTML = '<div class="gs-thank-you">' +
    '<h2>You\'re all set!</h2>' +
    '<p>Thanks for chatting with Nelly! Based on your goals, we\'ve picked the perfect plan for you. Start your free month — no commitment, cancel anytime.</p>' +
    '<a class="btn-primary" href="' + STRIPE_CHECKOUT_URL + '" target="_blank" style="padding:14px 40px;display:inline-block;margin-bottom:12px">Start Your Free Trial</a>' +
    '<p style="font-size:0.85rem;color:#536c7c;margin-top:8px">Already have the app? <a href="https://movementandmiles.ymove.app/p" target="_blank" style="color:#182241;font-weight:600">Open Movement & Miles</a></p>' +
    '</div>';
}

function gsAddBotMessage(text) {
  var msgs = document.getElementById('gs-msgs');
  var parsed = gsParseButtons(text);
  var b = document.createElement('div');
  b.className = 'gs-msg bot';
  b.textContent = parsed.text;
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
    body: JSON.stringify({ message: text, history: gsHistory.slice(0, -1) })
  }).then(function(res) {
    typing.classList.remove('show');
    if (!res.ok) throw new Error('Server error: ' + res.status);
    return res.json();
  }).then(function(data) {
    var reply = data.reply || "Sorry, couldn't connect right now. Try again!";
    gsHistory.push({ role: 'assistant', content: reply });
    // Check for lead data
    var result = gsStripLeadTag(reply);
    if (result.lead) {
      gsSaveLead(result.lead);
      if (result.text) gsAddBotMessage(result.text);
      setTimeout(gsShowThankYou, 2000);
    } else {
      gsAddBotMessage(reply);
    }
  }).catch(function(err) {
    typing.classList.remove('show');
    var b = document.createElement('div');
    b.className = 'gs-msg bot';
    b.textContent = 'Having trouble connecting right now. Try again in a moment!';
    msgs.appendChild(b);
    msgs.scrollTop = msgs.scrollHeight;
    console.error('Onboard error:', err);
  });
}

function gsSendMsg() {
  var input = document.getElementById('gs-input');
  var text = input.value.trim();
  if (!text) return;
  input.value = '';
  gsSendMsgText(text);
}
