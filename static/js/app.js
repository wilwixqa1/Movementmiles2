/* Movement & Miles — app.js — Routing, navigation, UI helpers */

var PAGE_PATHS = {
  home: '/',
  about: '/about-meg',
  training: '/training-programs',
  race: '/race-plans',
  store: '/store',
  detail: null
};

function showPage(name, skipPush) {
  document.querySelectorAll('.page').forEach(function(p) { p.classList.remove('active'); });
  document.querySelectorAll('.nav-links a').forEach(function(a) { a.classList.remove('active'); });
  document.getElementById('page-' + name).classList.add('active');
  var link = document.querySelector('.nav-links a[data-page="' + name + '"]');
  if (link) link.classList.add('active');
  window.scrollTo(0, 0);
  initFadeIns();
  if (!skipPush && PAGE_PATHS[name] !== undefined && PAGE_PATHS[name] !== null) {
    history.pushState({page: name}, '', PAGE_PATHS[name]);
  }
}

function showDetail(slug, skipPush) {
  var p = PROGRAMS.find(function(x) { return x.slug === slug; });
  if (!p) return;
  var tagClass = p.level === 'Beginner' ? 'tag-beginner' : p.level === 'Intermediate' ? 'tag-intermediate' : p.level === 'Advanced' ? 'tag-advanced' : 'tag-all';
  var imgEl = document.getElementById('detail-img');
  if (p.img) { imgEl.src = p.img; imgEl.style.display = 'block'; }
  else { imgEl.style.display = 'none'; }
  document.getElementById('detail-title').textContent = p.title;
  var tagEl = document.getElementById('detail-tag');
  tagEl.textContent = p.level;
  tagEl.className = 'tag ' + tagClass;
  document.getElementById('detail-duration').textContent = p.duration;
  document.getElementById('detail-weekly').innerHTML = '<strong>Each week has:</strong> ' + p.weekly;
  document.getElementById('detail-desc').textContent = p.desc;
  document.getElementById('detail-equip').innerHTML = '<strong>Equipment needed:</strong> ' + p.equip;
  showPage('detail', true);
  if (!skipPush) history.pushState({page: 'detail', slug: slug}, '', '/' + slug);
}

function toggleAcc(header) {
  var item = header.parentElement;
  var wasOpen = item.classList.contains('open');
  document.querySelectorAll('.accordion-item').forEach(function(i) { i.classList.remove('open'); });
  if (!wasOpen) item.classList.add('open');
}

function initFadeIns() {
  var obs = new IntersectionObserver(function(entries) {
    entries.forEach(function(e) { if (e.isIntersecting) e.target.classList.add('visible'); });
  }, { threshold: 0.08 });
  document.querySelectorAll('.page.active .fade-in:not(.visible)').forEach(function(el) { obs.observe(el); });
}

// URL-based routing on page load
(function() {
  var path = window.location.pathname.replace(/^\/+|\/+$/g, '');
  if (!path || path === 'home') { initFadeIns(); return; }
  var pathToPage = {
    'about-meg': 'about',
    'training-programs': 'training',
    'race-plans': 'race',
    'store': 'store'
  };
  if (pathToPage[path]) {
    showPage(pathToPage[path], true);
  } else {
    var prog = PROGRAMS.find(function(p) { return p.slug === path; });
    if (prog) showDetail(path, true);
  }
})();

// Back/forward button support
window.addEventListener('popstate', function(e) {
  if (e.state && e.state.page) {
    if (e.state.page === 'detail' && e.state.slug) {
      showDetail(e.state.slug, true);
    } else {
      showPage(e.state.page, true);
    }
  } else {
    showPage('home', true);
  }
});
