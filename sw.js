const CACHE_NAME = "aioti-v5";   // v5: guard/401 no plant.js + deep-link de alarme
// caminhos relativos ao sw.js: funcionam na raiz (produção) e em
// subpath (pipeline de teste github.io/USINA/)
const SHELL = [
  "./resumo.html",
  "./plant.html",
  "./os.html",
  "./index.html",
  "./css/style.css",
  "./css/layout.css",
  "./css/plant.css",
  "./css/login.css",
  "./os.css",
  "./js/app.js",
  "./js/plant.js",
  "./js/login.js",
  "./js/pwa.js",
  "./js/help_tour.js",
  "./js/notify_sound.js",
  "./manifest.json",
];

self.addEventListener("install", (e) => {
  e.waitUntil(
    caches.open(CACHE_NAME).then((c) => c.addAll(SHELL))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (e) => {
  if (e.request.method !== "GET") return;
  const url = new URL(e.request.url);
  if (url.origin !== location.origin) return;

  e.respondWith(
    fetch(e.request)
      .then((res) => {
        const clone = res.clone();
        caches.open(CACHE_NAME).then((c) => c.put(e.request, clone));
        return res;
      })
      .catch(() => caches.match(e.request))
  );
});

// ── Push Notifications ──
self.addEventListener("push", (e) => {
  let data = { title: "AIOTI Solar", body: "Nova notificação", url: "./resumo.html" };
  try {
    data = Object.assign(data, e.data.json());
  } catch (_) {}

  e.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: "./img/icon-192.png",
      badge: "./img/icon-192.png",
      tag: data.tag || "aioti-default",
      data: { url: data.url },
      vibrate: [200, 100, 200, 100, 300],
      silent: false,          // garante o som padrão do sistema (Android/iOS)
      renotify: true,         // mesma tag volta a tocar/vibrar em alarme repetido
      requireInteraction: data.priority === "high",
    })
  );
});

self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  const target = new URL(e.notification.data?.url || "./resumo.html", self.registration.scope).href;
  e.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
      // 🔑 A comparação era `c.url === target`, IGUALDADE EXATA. No celular
      // isso quase nunca casa: o app instalado costuma estar aberto no
      // resumo.html, ou na mesma usina mas sem o `action=alarms` da URL nova.
      // Não casando, caía no openWindow — que no Android apenas TRAZ A JANELA
      // EXISTENTE de volta, sem navegar. O usuário tocava na notificação e via
      // a tela em que já estava, como se o toque não tivesse feito nada.
      // Agora, havendo janela do app, ela é NAVEGADA para o alvo e focada.
      for (const c of list) {
        if (c.url === target && "focus" in c) return c.focus();
      }
      const app = list.find((c) => c.url.indexOf(self.registration.scope) === 0);
      if (app && "navigate" in app) {
        return app.navigate(target).then((c) => (c || app).focus()).catch(() => clients.openWindow(target));
      }
      return clients.openWindow(target);
    })
  );
});
