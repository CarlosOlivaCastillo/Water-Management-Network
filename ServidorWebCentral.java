import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ServidorWebCentral {

    private static HttpServer servidorHttp;
    private static KafkaProducer<String, String> producerKafka;
    private static final Map<String, DatosTelemetria> mapaTelemetria = new ConcurrentHashMap<>();
    private static final List<String> logsEventos = Collections.synchronizedList(new ArrayList<>());

    public static class DatosTelemetria {
        public String idWS;
        public double caudal;
        public double volumenAcumulado;
        public long timestamp;

        public DatosTelemetria(String idWS, double caudal, double volumenAcumulado, long timestamp) {
            this.idWS = idWS;
            this.caudal = caudal;
            this.volumenAcumulado = volumenAcumulado;
            this.timestamp = timestamp;
        }
    }

    public static void registrarEvento(String evento) {
        String timestamp = new java.text.SimpleDateFormat("HH:mm:ss").format(new Date());
        String logEntry = "[" + timestamp + "] " + evento;
        logsEventos.add(logEntry);
        if (logsEventos.size() > 100) {
            logsEventos.remove(0);
        }
    }

    public static void actualizarTelemetria(String idWS, double caudal, double volumen, long ts) {
        mapaTelemetria.put(idWS, new DatosTelemetria(idWS, caudal, volumen, ts));
        registrarEvento("Telemetría de " + idWS + ": Caudal " + String.format(Locale.US, "%.1f", caudal) + " L/s | Acum: " + String.format(Locale.US, "%.1f", volumen) + " L");
    }

    public static void iniciar(int puertoHttp, String ipKafka, int puertoKafka) {
        try {
            // Inicializar productor de órdenes para control desde la web
            inicializarProducerKafka(ipKafka, puertoKafka);

            servidorHttp = HttpServer.create(new InetSocketAddress(puertoHttp), 0);
            servidorHttp.setExecutor(Executors.newCachedThreadPool());

            // Rutas de la API y Dashboard
            servidorHttp.createContext("/", new DashboardHandler());
            servidorHttp.createContext("/api/estaciones", new EstacionesApiHandler());
            servidorHttp.createContext("/api/telemetria", new TelemetriaApiHandler());
            servidorHttp.createContext("/api/logs", new LogsApiHandler());
            servidorHttp.createContext("/api/riego", new ControlRiegoApiHandler());
            servidorHttp.createContext("/api/emergencia", new ParadaEmergenciaApiHandler());

            servidorHttp.start();
            System.out.println("[WEB DASHBOARD] Servidor Web activo en: http://localhost:" + puertoHttp + "/");
            registrarEvento("Servidor Web Dashboard iniciado en puerto " + puertoHttp);
        } catch (IOException e) {
            System.err.println("[WEB DASHBOARD] No se pudo iniciar el servidor web en el puerto " + puertoHttp + ": " + e.getMessage());
        }
    }

    private static void inicializarProducerKafka(String ipKafka, int puertoKafka) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);

        try {
            producerKafka = new KafkaProducer<>(props);
        } catch (Exception e) {
            System.err.println("[WEB DASHBOARD] Aviso: Productor Kafka no disponible para la web (" + e.getMessage() + ")");
        }
    }

    public static void detener() {
        if (servidorHttp != null) {
            servidorHttp.stop(1);
        }
        if (producerKafka != null) {
            try {
                producerKafka.close();
            } catch (Exception ignored) {}
        }
    }

    // --- HANDLERS HTTP ---

    private static class DashboardHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] contenidoHtml = HTML_DASHBOARD.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, contenidoHtml.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(contenidoHtml);
            }
        }
    }

    private static class EstacionesApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            configurarCORS(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            List<String> rawEstaciones = GestorCentral.getInstance().obtenerEstaciones();
            StringBuilder json = new StringBuilder("[");

            for (int i = 0; i < rawEstaciones.size(); i++) {
                String[] partes = rawEstaciones.get(i).split("#");
                String id = partes.length > 0 ? partes[0] : "";
                String estado = partes.length > 1 ? partes[1] : "";
                String ubicacion = partes.length > 2 ? partes[2] : "";
                String operador = partes.length > 3 ? partes[3] : "-";

                DatosTelemetria telem = mapaTelemetria.get(id);
                double caudal = telem != null ? telem.caudal : 0.0;
                double volumen = telem != null ? telem.volumenAcumulado : 0.0;

                json.append("{")
                    .append("\"idWS\":\"").append(id).append("\",")
                    .append("\"estado\":\"").append(estado).append("\",")
                    .append("\"ubicacion\":\"").append(ubicacion).append("\",")
                    .append("\"operador\":\"").append(operador).append("\",")
                    .append("\"caudal\":").append(String.format(Locale.US, "%.2f", caudal)).append(",")
                    .append("\"volumen\":").append(String.format(Locale.US, "%.2f", volumen))
                    .append("}");

                if (i < rawEstaciones.size() - 1) {
                    json.append(",");
                }
            }
            json.append("]");

            byte[] resp = json.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        }
    }

    private static class TelemetriaApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            configurarCORS(exchange);
            StringBuilder json = new StringBuilder("{");
            int count = 0;
            for (Map.Entry<String, DatosTelemetria> entry : mapaTelemetria.entrySet()) {
                DatosTelemetria d = entry.getValue();
                if (count > 0) json.append(",");
                json.append("\"").append(entry.getKey()).append("\":{")
                    .append("\"caudal\":").append(String.format(Locale.US, "%.2f", d.caudal)).append(",")
                    .append("\"volumen\":").append(String.format(Locale.US, "%.2f", d.volumenAcumulado)).append(",")
                    .append("\"timestamp\":").append(d.timestamp)
                    .append("}");
                count++;
            }
            json.append("}");

            byte[] resp = json.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        }
    }

    private static class LogsApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            configurarCORS(exchange);
            StringBuilder json = new StringBuilder("[");
            synchronized (logsEventos) {
                for (int i = 0; i < logsEventos.size(); i++) {
                    json.append("\"").append(logsEventos.get(i).replace("\"", "\\\"")).append("\"");
                    if (i < logsEventos.size() - 1) json.append(",");
                }
            }
            json.append("]");

            byte[] resp = json.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        }
    }

    private static class ControlRiegoApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            configurarCORS(exchange);
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            StringBuilder sb = new StringBuilder();
            String linea;
            while ((linea = br.readLine()) != null) sb.append(linea);

            // Parse simple body: idWS=WS_01&accion=START&duracion=10
            String body = sb.toString();
            Map<String, String> params = parseFormBody(body);

            String idWS = params.get("idWS");
            String accion = params.get("accion");
            int duracion = 10;
            try {
                if (params.containsKey("duracion")) {
                    duracion = Integer.parseInt(params.get("duracion"));
                }
            } catch (Exception ignored) {}

            boolean ok = false;
            String msg = "";

            if ("START".equalsIgnoreCase(accion) && idWS != null) {
                if (GestorCentral.getInstance().autorizarRiego(idWS, "WEB_PANEL", duracion)) {
                    enviarOrdenKafka("INICIAR_RIEGO#" + idWS);
                    registrarEvento("Orden WEB: Riego iniciado en " + idWS + " por " + duracion + "s");
                    ok = true;
                    msg = "Riego activado correctamente";

                    // Temporizador para parada automática
                    final int d = duracion;
                    final String ws = idWS;
                    new Thread(() -> {
                        try {
                            Thread.sleep(d * 1000L);
                            GestorCentral.getInstance().actualizarEstado(ws, "DISPONIBLE");
                            enviarOrdenKafka("PARAR_RIEGO#" + ws);
                            registrarEvento("Fin temporizado de riego WEB para " + ws);
                        } catch (Exception ignored) {}
                    }).start();
                } else {
                    msg = "No se pudo autorizar el riego (estación ocupada o en avería)";
                }
            } else if ("STOP".equalsIgnoreCase(accion) && idWS != null) {
                GestorCentral.getInstance().actualizarEstado(idWS, "DISPONIBLE");
                enviarOrdenKafka("PARAR_RIEGO#" + idWS);
                registrarEvento("Orden WEB: Riego detenido en " + idWS);
                ok = true;
                msg = "Riego detenido";
            }

            String respuesta = "{\"success\":" + ok + ",\"message\":\"" + msg + "\"}";
            byte[] resp = respuesta.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        }
    }

    private static class ParadaEmergenciaApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            configurarCORS(exchange);
            List<String> estaciones = GestorCentral.getInstance().obtenerEstaciones();
            for (String e : estaciones) {
                String[] partes = e.split("#");
                if (partes.length > 0) {
                    String id = partes[0];
                    GestorCentral.getInstance().actualizarEstado(id, "DISPONIBLE");
                }
            }
            enviarOrdenKafka("PARAR_RIEGO#ALL");
            registrarEvento("🚨 PARADA DE EMERGENCIA GLOBAL EJECUTADA");

            String respuesta = "{\"success\":true,\"message\":\"Todas las electroválvulas han sido cerradas\"}";
            byte[] resp = respuesta.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        }
    }

    private static void enviarOrdenKafka(String orden) {
        if (producerKafka != null) {
            try {
                producerKafka.send(new ProducerRecord<>("ordenes-riego", orden));
            } catch (Exception e) {
                System.err.println("[WEB DASHBOARD] Error enviando a Kafka: " + e.getMessage());
            }
        }
    }

    private static Map<String, String> parseFormBody(String body) {
        Map<String, String> map = new HashMap<>();
        if (body == null || body.isEmpty()) return map;
        String[] pairs = body.split("&");
        for (String pair : pairs) {
            String[] kv = pair.split("=");
            if (kv.length == 2) {
                map.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                        URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
            }
        }
        return map;
    }

    private static void configurarCORS(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    // --- PLANTILLA HTML/CSS/JS DEL DASHBOARD WEB ---
    private static final String HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Water Management Network - Central Dashboard</title>
    <!-- Tailwind CSS CDN -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- Chart.js CDN -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        @keyframes pulse-fast { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.7; transform: scale(1.05); } }
        .animate-fuga { animation: pulse-fast 1s infinite; }
        .water-wave { background: linear-gradient(135deg, #0ea5e9, #2563eb); }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 font-sans min-h-screen">

    <!-- Navbar -->
    <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-50 px-6 py-4 flex items-center justify-between shadow-lg">
        <div class="flex items-center space-x-3">
            <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center text-white shadow-cyan-500/30 shadow-md">
                <i class="fa-solid fa-droplet text-xl"></i>
            </div>
            <div>
                <h1 class="text-xl font-bold tracking-tight bg-gradient-to-r from-cyan-400 to-blue-400 bg-clip-text text-transparent">Water Management Network</h1>
                <p class="text-xs text-slate-400">Panel Centralizado de Telemetría y Control de Riego</p>
            </div>
        </div>
        <div class="flex items-center space-x-4">
            <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                <span class="w-2 h-2 rounded-full bg-emerald-400 mr-2 animate-pulse"></span> Sistema en Línea
            </span>
            <button onclick="paradaEmergencia()" class="px-4 py-2 bg-rose-600 hover:bg-rose-700 text-white rounded-lg text-sm font-semibold shadow-lg shadow-rose-600/30 transition flex items-center space-x-2">
                <i class="fa-solid fa-triangle-exclamation"></i>
                <span>Parada de Emergencia</span>
            </button>
        </div>
    </header>

    <main class="max-w-7xl mx-auto p-6 space-y-6">

        <!-- Métricas Principales -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm">
                <div class="flex items-center justify-between text-slate-400 text-sm font-medium mb-2">
                    <span>Estaciones Totales</span>
                    <i class="fa-solid fa-network-wired text-cyan-400"></i>
                </div>
                <div id="stat-total" class="text-3xl font-extrabold text-white">0</div>
                <div class="text-xs text-slate-500 mt-1">Registradas en Base de Datos</div>
            </div>
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm">
                <div class="flex items-center justify-between text-slate-400 text-sm font-medium mb-2">
                    <span>En Riego Activo</span>
                    <i class="fa-solid fa-water text-blue-400 animate-bounce"></i>
                </div>
                <div id="stat-regando" class="text-3xl font-extrabold text-blue-400">0</div>
                <div class="text-xs text-slate-500 mt-1">Consumiendo agua en tiempo real</div>
            </div>
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm">
                <div class="flex items-center justify-between text-slate-400 text-sm font-medium mb-2">
                    <span>Disponibles</span>
                    <i class="fa-solid fa-circle-check text-emerald-400"></i>
                </div>
                <div id="stat-disponibles" class="text-3xl font-extrabold text-emerald-400">0</div>
                <div class="text-xs text-slate-500 mt-1">Listas para recibir órdenes</div>
            </div>
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm">
                <div class="flex items-center justify-between text-slate-400 text-sm font-medium mb-2">
                    <span>Alertas / Fugas</span>
                    <i class="fa-solid fa-triangle-exclamation text-rose-400"></i>
                </div>
                <div id="stat-fugas" class="text-3xl font-extrabold text-rose-400">0</div>
                <div class="text-xs text-slate-500 mt-1">Requieren mantenimiento</div>
            </div>
        </div>

        <!-- Grid de Estaciones y Gráfico -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">

            <!-- Lista de Tarjetas de Estaciones (2 columnas en LG) -->
            <div class="lg:col-span-2 space-y-4">
                <div class="flex items-center justify-between">
                    <h2 class="text-lg font-bold text-slate-200 flex items-center space-x-2">
                        <i class="fa-solid fa-satellite-dish text-cyan-400"></i>
                        <span>Estaciones de Riego Monitoreadas</span>
                    </h2>
                    <span class="text-xs text-slate-500">Actualización automática cada 1s</span>
                </div>

                <div id="grid-estaciones" class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <!-- Tarjetas generadas dinámicamente -->
                </div>
            </div>

            <!-- Gráfico y Consola de Eventos -->
            <div class="space-y-6">
                <!-- Gráfico de Consumo -->
                <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm">
                    <h3 class="text-sm font-bold text-slate-300 mb-4 flex items-center space-x-2">
                        <i class="fa-solid fa-chart-line text-cyan-400"></i>
                        <span>Caudal en Vivo (Litros/segundo)</span>
                    </h3>
                    <div class="h-56">
                        <canvas id="chartCaudal"></canvas>
                    </div>
                </div>

                <!-- Feed de Logs / Eventos -->
                <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm">
                    <div class="flex items-center justify-between mb-3">
                        <h3 class="text-sm font-bold text-slate-300 flex items-center space-x-2">
                            <i class="fa-solid fa-terminal text-emerald-400"></i>
                            <span>Registro de Actividad en Vivo</span>
                        </h3>
                        <button onclick="limpiarLogs()" class="text-xs text-slate-500 hover:text-slate-300">Limpiar</button>
                    </div>
                    <div id="console-logs" class="bg-slate-950 p-3 rounded-xl font-mono text-xs text-slate-400 h-48 overflow-y-auto space-y-1 border border-slate-800/80">
                        <div>[00:00:00] Esperando conexión de eventos...</div>
                    </div>
                </div>
            </div>

        </div>

    </main>

    <script>
        let chart = null;
        let chartData = { labels: [], datasets: [] };

        function initChart() {
            const ctx = document.getElementById('chartCaudal').getContext('2d');
            chart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Caudal Actual (L/s)',
                        data: [],
                        backgroundColor: 'rgba(14, 165, 233, 0.6)',
                        borderColor: '#0284c7',
                        borderWidth: 1.5,
                        borderRadius: 6
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true,
                            max: 20,
                            grid: { color: 'rgba(51, 65, 85, 0.4)' },
                            ticks: { color: '#94a3b8' }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { color: '#94a3b8' }
                        }
                    },
                    plugins: {
                        legend: { display: false }
                    }
                }
            });
        }

        async function actualizarDatos() {
            try {
                // 1. Obtener Estaciones
                const resp = await fetch('/api/estaciones');
                const estaciones = await resp.json();

                let regando = 0, disponibles = 0, fugas = 0;
                const grid = document.getElementById('grid-estaciones');
                grid.innerHTML = '';

                const labels = [];
                const caudales = [];

                if (estaciones.length === 0) {
                    grid.innerHTML = `<div class="col-span-2 text-center p-8 bg-slate-900 border border-slate-800 rounded-2xl text-slate-500">
                        <i class="fa-solid fa-inbox text-3xl mb-2"></i>
                        <p>No hay estaciones registradas. Inicia un Monitor (WM_WS_M) para conectar una estación.</p>
                    </div>`;
                }

                estaciones.forEach(e => {
                    if (e.estado === 'REGANDO') regando++;
                    else if (e.estado === 'DISPONIBLE') disponibles++;
                    else if (e.estado === 'FUGA') fugas++;

                    labels.push(e.idWS);
                    caudales.push(e.caudal || 0);

                    // Color de tarjeta
                    let badgeClass = 'bg-slate-700 text-slate-300';
                    let borderClass = 'border-slate-800';
                    let icon = 'fa-power-off text-slate-500';

                    if (e.estado === 'DISPONIBLE') {
                        badgeClass = 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
                        borderClass = 'border-emerald-500/30';
                        icon = 'fa-circle-check text-emerald-400';
                    } else if (e.estado === 'REGANDO') {
                        badgeClass = 'bg-blue-500/20 text-blue-400 border border-blue-500/30';
                        borderClass = 'border-blue-500/50 shadow-lg shadow-blue-500/10';
                        icon = 'fa-water text-blue-400 animate-pulse';
                    } else if (e.estado === 'FUGA') {
                        badgeClass = 'bg-rose-500/20 text-rose-400 border border-rose-500/30 animate-fuga';
                        borderClass = 'border-rose-500/50 shadow-lg shadow-rose-500/10';
                        icon = 'fa-triangle-exclamation text-rose-400';
                    }

                    const card = document.createElement('div');
                    card.className = `bg-slate-900 border ${borderClass} rounded-2xl p-5 space-y-4 transition duration-300`;
                    card.innerHTML = `
                        <div class="flex items-center justify-between">
                            <div class="flex items-center space-x-2">
                                <i class="fa-solid ${icon}"></i>
                                <span class="font-bold text-lg text-white">${e.idWS}</span>
                            </div>
                            <span class="px-2.5 py-0.5 rounded-full text-xs font-semibold ${badgeClass}">
                                ${e.estado}
                            </span>
                        </div>

                        <div class="text-xs text-slate-400">
                            <div class="flex justify-between py-0.5">
                                <span>Ubicación:</span>
                                <span class="font-medium text-slate-200">${e.ubicacion}</span>
                            </div>
                            <div class="flex justify-between py-0.5">
                                <span>Operador actual:</span>
                                <span class="font-medium text-slate-200">${e.operador || '-'}</span>
                            </div>
                        </div>

                        <div class="grid grid-cols-2 gap-2 bg-slate-950/70 p-3 rounded-xl border border-slate-800/80">
                            <div>
                                <span class="text-[10px] text-slate-500 uppercase font-semibold">Caudal</span>
                                <div class="text-base font-bold text-cyan-400">${e.caudal.toFixed(1)} <span class="text-xs font-normal text-slate-400">L/s</span></div>
                            </div>
                            <div>
                                <span class="text-[10px] text-slate-500 uppercase font-semibold">Volumen Total</span>
                                <div class="text-base font-bold text-blue-400">${e.volumen.toFixed(1)} <span class="text-xs font-normal text-slate-400">L</span></div>
                            </div>
                        </div>

                        <div class="flex space-x-2 pt-1">
                            ${e.estado === 'DISPONIBLE' ? `
                                <button onclick="iniciarRiegoPrompt('${e.idWS}')" class="flex-1 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-xs font-bold transition flex items-center justify-center space-x-1">
                                    <i class="fa-solid fa-play text-[10px]"></i>
                                    <span>Regar</span>
                                </button>
                            ` : ''}
                            ${e.estado === 'REGANDO' ? `
                                <button onclick="detenerRiego('${e.idWS}')" class="flex-1 py-2 bg-rose-600 hover:bg-rose-700 text-white rounded-lg text-xs font-bold transition flex items-center justify-center space-x-1">
                                    <i class="fa-solid fa-stop text-[10px]"></i>
                                    <span>Detener</span>
                                </button>
                            ` : ''}
                        </div>
                    `;
                    grid.appendChild(card);
                });

                document.getElementById('stat-total').innerText = estaciones.length;
                document.getElementById('stat-regando').innerText = regando;
                document.getElementById('stat-disponibles').innerText = disponibles;
                document.getElementById('stat-fugas').innerText = fugas;

                // Actualizar gráfico
                if (chart) {
                    chart.data.labels = labels;
                    chart.data.datasets[0].data = caudales;
                    chart.update('none');
                }

                // 2. Obtener Logs
                const respLogs = await fetch('/api/logs');
                const logs = await respLogs.json();
                const logContainer = document.getElementById('console-logs');
                if (logs.length > 0) {
                    logContainer.innerHTML = logs.map(l => `<div>${l}</div>`).join('');
                    logContainer.scrollTop = logContainer.scrollHeight;
                }

            } catch (err) {
                console.error("Error al refrescar dashboard:", err);
            }
        }

        async function iniciarRiegoPrompt(idWS) {
            const duracion = prompt(`Introduce duración de riego en segundos para ${idWS}:`, "10");
            if (!duracion) return;
            try {
                const resp = await fetch('/api/riego', {
                    method: 'POST',
                    body: `idWS=${encodeURIComponent(idWS)}&accion=START&duracion=${encodeURIComponent(duracion)}`
                });
                const res = await resp.json();
                if (!res.success) alert(res.message);
                actualizarDatos();
            } catch (err) {
                alert("Error al enviar orden: " + err);
            }
        }

        async function detenerRiego(idWS) {
            try {
                await fetch('/api/riego', {
                    method: 'POST',
                    body: `idWS=${encodeURIComponent(idWS)}&accion=STOP`
                });
                actualizarDatos();
            } catch (err) {
                alert("Error al detener riego: " + err);
            }
        }

        async function paradaEmergencia() {
            if (confirm("¿Confirmas la parada de emergencia de TODAS las estaciones de riego?")) {
                await fetch('/api/emergencia', { method: 'POST' });
                actualizarDatos();
            }
        }

        function limpiarLogs() {
            document.getElementById('console-logs').innerHTML = '';
        }

        window.onload = () => {
            initChart();
            actualizarDatos();
            setInterval(actualizarDatos, 1000);
        };
    </script>
</body>
</html>
    """;
}
