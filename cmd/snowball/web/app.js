(() => {
  "use strict";
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // --- Map ---
  const map = L.map("map", { zoomControl: true, worldCopyJump: true }).setView([20, 0], 2);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: "© OpenStreetMap, © CARTO",
    subdomains: "abcd",
    maxZoom: 19,
  }).addTo(map);
  const heat = L.heatLayer([], { radius: 18, blur: 22, maxZoom: 12 }).addTo(map);
  const heatPoints = [];
  const HEAT_CAP = 20000;

  function addPoints(points) {
    for (const p of points) {
      if (typeof p.lat !== "number" || typeof p.lon !== "number") continue;
      heatPoints.push([p.lat, p.lon, 0.6]);
      if (!reduceMotion) flashPing(p.lat, p.lon);
    }
    if (heatPoints.length > HEAT_CAP) heatPoints.splice(0, heatPoints.length - HEAT_CAP);
    heat.setLatLngs(heatPoints);
  }

  function flashPing(lat, lon) {
    const m = L.circleMarker([lat, lon], {
      radius: 5, color: "#22C55E", weight: 1, fillColor: "#22C55E", fillOpacity: 0.9,
    }).addTo(map);
    let op = 0.9;
    const id = setInterval(() => {
      op -= 0.09;
      if (op <= 0) { clearInterval(id); map.removeLayer(m); return; }
      m.setStyle({ fillOpacity: op, opacity: op });
    }, 80);
  }

  // --- Sparkline (rate) ---
  const sparkData = [[], []];
  const SPARK_LEN = 120;
  let t = 0;
  const spark = new uPlot({
    width: 70, height: 24, cursor: { show: false }, legend: { show: false },
    axes: [{ show: false }, { show: false }],
    scales: { x: { time: false } },
    series: [{}, { stroke: "#22C55E", width: 1.5, fill: "rgba(34,197,94,0.15)" }],
  }, sparkData, document.getElementById("spark"));

  function pushRate(rate) {
    sparkData[0].push(t++); sparkData[1].push(rate);
    if (sparkData[0].length > SPARK_LEN) { sparkData[0].shift(); sparkData[1].shift(); }
    spark.setData(sparkData);
  }

  // --- Feed ---
  const feed = document.getElementById("feed");
  const FEED_CAP = 60;
  function pushFeed(points) {
    for (const p of points.slice(-12)) {
      const li = document.createElement("li");
      const mac = document.createElement("span"); mac.className = "mac"; mac.textContent = p.b;
      const loc = document.createElement("span"); loc.className = "loc";
      loc.textContent = p.lat.toFixed(3) + ", " + p.lon.toFixed(3);
      li.append(mac, loc);
      feed.prepend(li);
    }
    while (feed.children.length > FEED_CAP) feed.removeChild(feed.lastChild);
  }

  // --- Stats / status ---
  const el = (id) => document.getElementById(id);
  let sliderActive = false;
  function applyStats(s) {
    el("m-aps").textContent = s.aps.toLocaleString();
    el("m-rate").textContent = s.rate.toFixed(1);
    el("m-pending").textContent = s.pending.toLocaleString();
    el("m-inflight").textContent = s.inflight.toLocaleString();
    el("workers-active").textContent = s.workers;
    pushRate(s.rate);

    const slider = el("workers");
    el("workers-max").textContent = s.max;
    slider.max = String(s.max);
    if (!sliderActive) { slider.value = String(s.target); el("workers-val").textContent = s.target; }

    const status = el("status");
    const text = el("status-text");
    status.classList.remove("running", "throttled", "down");
    if (s.throttled) { status.classList.add("throttled"); text.textContent = "THROTTLED (" + Math.round(s.backoffMs / 1000) + "s)"; }
    else { status.classList.add("running"); text.textContent = "RUNNING"; }
  }

  // --- Workers control ---
  const slider = el("workers");
  slider.addEventListener("input", () => { sliderActive = true; el("workers-val").textContent = slider.value; });
  let sendTimer = null;
  slider.addEventListener("change", () => {
    sliderActive = false;
    clearTimeout(sendTimer);
    sendTimer = setTimeout(() => send({ type: "setWorkers", n: Number(slider.value) }), 50);
  });

  // --- WebSocket ---
  let ws = null;
  function send(obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
  function setDown() {
    const status = el("status");
    status.classList.remove("running", "throttled"); status.classList.add("down");
    el("status-text").textContent = "DISCONNECTED";
  }
  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(proto + "://" + location.host + "/ws" + location.search);
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "snapshot") { applyStats(msg.stats); addPoints(msg.points); }
      else if (msg.type === "stats") { applyStats(msg); }
      else if (msg.type === "capture") { addPoints(msg.points); pushFeed(msg.points); }
    };
    ws.onclose = () => { setDown(); setTimeout(connect, 2000); };
    ws.onerror = () => ws.close();
  }
  connect();
})();
