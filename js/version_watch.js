/* =========================================================================
   AVISO DE VERSÃO NOVA
   Criado em 07/08/2026.

   POR QUE EXISTE
   Um deploy deste dia mudou o contrato de autenticação da API. As abas que já
   estavam abertas continuaram rodando o JS antigo em memória, batendo na API a
   cada 10 segundos e recebendo 401: dashboard congelado, sem mensagem nenhuma,
   por horas. Ninguém tinha como adivinhar que a solução era um F5.

   Não existe jeito de o servidor forçar reload numa aba aberta, e avisar no
   WhatsApp só alcança quem lê. Este arquivo faz a própria página perceber que
   saiu versão nova e oferecer o botão.

   COMO DETECTA
   O GitHub Pages carimba o MESMO Last-Modified em todos os arquivos do site a
   cada publicação (comprovado no incidente de 06/08). Então um HEAD em
   qualquer arquivo do próprio site já denuncia um deploy — sem precisar de
   endpoint novo, de número de versão para lembrar de incrementar, nem de
   arquivo extra para manter.

   🔑 A sonda é no MESMO ORIGIN, de propósito, e NÃO passa pela API. É o que
   faz o aviso funcionar justamente quando a API está recusando — que é
   exatamente a situação em que ele mais importa. Se dependesse da API, ficaria
   mudo no único cenário para o qual foi feito.
   ========================================================================= */
(function () {
  "use strict";

  var SONDA = "manifest.json";          // qualquer arquivo do site serve
  var INTERVALO_MS = 4 * 60 * 1000;     // 4 min: o custo é um HEAD sem corpo
  var carimboInicial = null;
  var avisoNaTela = false;

  function carimbo() {
    return fetch(SONDA, { method: "HEAD", cache: "no-store" })
      .then(function (r) {
        if (!r.ok) return null;
        return r.headers.get("last-modified") || r.headers.get("etag");
      })
      .catch(function () { return null; });   // rede oscilando não é versão nova
  }

  function mostrarAviso() {
    if (avisoNaTela || document.getElementById("aiotiVersionBar")) return;
    avisoNaTela = true;

    var bar = document.createElement("div");
    bar.id = "aiotiVersionBar";
    bar.setAttribute("role", "status");
    bar.style.cssText = [
      "position:fixed", "top:0", "left:0", "right:0", "z-index:2147483647",
      "background:#1565c0", "color:#fff",
      "font:600 14px/1.4 system-ui,-apple-system,Segoe UI,Roboto,sans-serif",
      "padding:10px 16px", "display:flex", "align-items:center", "gap:12px",
      "flex-wrap:wrap", "justify-content:center",
      "box-shadow:0 2px 10px rgba(0,0,0,.25)"
    ].join(";");

    var txt = document.createElement("span");
    txt.textContent = "Saiu uma atualização da plataforma. Esta aba está desatualizada e pode parar de receber dados novos.";

    var btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = "Atualizar agora";
    btn.style.cssText = [
      "background:#fff", "color:#1565c0", "border:0", "border-radius:6px",
      "padding:7px 16px", "font:700 14px system-ui,sans-serif", "cursor:pointer"
    ].join(";");
    btn.addEventListener("click", function () { window.location.reload(); });

    var depois = document.createElement("button");
    depois.type = "button";
    depois.textContent = "Depois";
    depois.style.cssText = [
      "background:transparent", "color:#cfe3ff", "border:0",
      "font:400 13px system-ui,sans-serif", "cursor:pointer",
      "text-decoration:underline"
    ].join(";");
    // "Depois" some a barra mas NÃO desliga a checagem: se a pessoa ignorar e
    // continuar com a aba velha, o aviso volta na próxima rodada. O objetivo é
    // não deixar ninguém preso numa tela morta sem saber.
    depois.addEventListener("click", function () {
      bar.remove();
      avisoNaTela = false;
    });

    bar.appendChild(txt);
    bar.appendChild(btn);
    bar.appendChild(depois);
    (document.body || document.documentElement).appendChild(bar);
  }

  function checar() {
    carimbo().then(function (atual) {
      if (!atual) return;
      if (carimboInicial === null) { carimboInicial = atual; return; }
      if (atual !== carimboInicial) mostrarAviso();
    });
  }

  function iniciar() {
    carimbo().then(function (inicial) {
      carimboInicial = inicial;              // pode ser null; checar() se ajeita
      setInterval(checar, INTERVALO_MS);
      // Voltar para a aba é o momento mais provável de ela estar velha:
      // confere na hora em vez de esperar o próximo ciclo.
      document.addEventListener("visibilitychange", function () {
        if (!document.hidden) checar();
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", iniciar);
  } else {
    iniciar();
  }
})();
