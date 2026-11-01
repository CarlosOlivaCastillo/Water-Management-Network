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
        registrarEvento("Telemetría " + idWS + ": " + String.format(Locale.US, "%.1f", caudal * 60) + " L/min | Acum: " + String.format(Locale.US, "%.1f", volumen) + " L");
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
            servidorHttp.createContext("/api/estado", new CambiarEstadoApiHandler());
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
                    .append("\"caudalLMin\":").append(String.format(Locale.US, "%.2f", caudal * 60)).append(",")
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
                List<String> objetivos = "ALL".equalsIgnoreCase(idWS)
                        ? GestorCentral.getInstance().obtenerIdsEstaciones()
                        : Collections.singletonList(idWS);
                int iniciadas = 0;
                for (String target : objetivos) {
                    if (GestorCentral.getInstance().autorizarRiego(target, "WEB_PANEL", duracion)) {
                        enviarOrdenKafka("INICIAR_RIEGO#" + target + "#" + duracion);
                        registrarEvento("Orden WEB: Riego iniciado en " + target + " por " + duracion + "s");
                        iniciadas++;

                        final int d = duracion;
                        final String ws = target;
                        new Thread(() -> {
                            try {
                                Thread.sleep(d * 1000L);
                                GestorCentral.getInstance().actualizarEstado(ws, "DISPONIBLE");
                                enviarOrdenKafka("PARAR_RIEGO#" + ws);
                                registrarEvento("Fin temporizado de riego WEB para " + ws);
                            } catch (Exception ignored) {}
                        }).start();
                    }
                }
                ok = iniciadas > 0;
                msg = ok ? "Riego activado en " + iniciadas + " estación(es)" : "Ninguna estación estaba disponible para iniciar riego";
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

    private static class CambiarEstadoApiHandler implements HttpHandler {
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

            Map<String, String> params = parseFormBody(sb.toString());
            String idWS = params.get("idWS");
            String accion = params.get("accion"); // BLOQUEAR / ACTIVAR

            boolean ok = false;
            String msg = "";

            if ("BLOQUEAR".equalsIgnoreCase(accion) && idWS != null) {
                WM_Central.bloquearEstacion(idWS);
                ok = true;
                msg = "Estación(es) puesta(s) Out of Service";
            } else if ("ACTIVAR".equalsIgnoreCase(accion) && idWS != null) {
                WM_Central.activarEstacion(idWS);
                ok = true;
                msg = "Estación(es) activada(s)";
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
                if (partes.length > 1 && "REGANDO".equalsIgnoreCase(partes[1].trim())) {
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
                try {
                    map.put(URLDecoder.decode(kv[0], "UTF-8"),
                            URLDecoder.decode(kv[1], "UTF-8"));
                } catch (UnsupportedEncodingException ignored) {}
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
    private static final String HTML_DASHBOARD = 
        "\n" +
        "<!DOCTYPE html>\n" +
        "<html lang=\"es\">\n" +
        "<head>\n" +
        "    <meta charset=\"UTF-8\">\n" +
        "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
        "    <title>Water Management Network - Central Dashboard</title>\n" +
        "    <!-- Tailwind CSS CDN -->\n" +
        "    <script src=\"https://cdn.tailwindcss.com\"></script>\n" +
        "    <!-- Chart.js CDN -->\n" +
        "    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n" +
        "    <link href=\"https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css\" rel=\"stylesheet\">\n" +
        "    <style>\n" +
        "        @keyframes pulse-fast { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.6; transform: scale(1.05); } }\n" +
        "        @keyframes flow-pipe { from { stroke-dashoffset: 24; } to { stroke-dashoffset: 0; } }\n" +
        "        @keyframes blink-green { 0%, 100% { background-color: rgba(16, 185, 129, 0.35); border-color: #10b981; } 50% { background-color: rgba(5, 150, 105, 0.85); border-color: #34d399; box-shadow: 0 0 20px rgba(16, 185, 129, 0.6); } }\n" +
        "        \n" +
        "        .animate-fuga { animation: pulse-fast 0.8s infinite; }\n" +
        "        .animate-regando-card { animation: blink-green 1.2s infinite ease-in-out; }\n" +
        "        .pipe-flow { animation: flow-pipe 0.8s linear infinite; stroke-dasharray: 8 4; }\n" +
        "    </style>\n" +
        "</head>\n" +
        "<body class=\"bg-slate-950 text-slate-100 font-sans min-h-screen\">\n" +
        "\n" +
        "    <!-- Navbar -->\n" +
        "    <header class=\"border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-50 px-6 py-4 flex items-center justify-between shadow-lg\">\n" +
        "        <div class=\"flex items-center space-x-3\">\n" +
        "            <div class=\"w-10 h-10 rounded-xl bg-gradient-to-tr from-emerald-500 to-teal-600 flex items-center justify-center text-white shadow-emerald-500/30 shadow-md\">\n" +
        "                <i class=\"fa-solid fa-droplet text-xl\"></i>\n" +
        "            </div>\n" +
        "            <div>\n" +
        "                <h1 class=\"text-xl font-bold tracking-tight bg-gradient-to-r from-emerald-400 to-cyan-400 bg-clip-text text-transparent\">Water Management Network</h1>\n" +
        "                <p class=\"text-xs text-slate-400\">Cuadro de Mandos Centralizado de Riego (Especificación Oficial SD)</p>\n" +
        "            </div>\n" +
        "        </div>\n" +
        "        <div class=\"flex items-center space-x-4\">\n" +
        "            <span class=\"inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/30\">\n" +
        "                <span class=\"w-2 h-2 rounded-full bg-emerald-400 mr-2 animate-pulse\"></span> Sistema en Línea\n" +
        "            </span>\n" +
        "            <button onclick=\"iniciarRiegoPrompt('ALL')\" class=\"px-3 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-xs font-semibold transition flex items-center space-x-1.5\">\n" +
        "                <i class=\"fa-solid fa-play\"></i>\n" +
        "                <span>Regar Todas</span>\n" +
        "            </button>\n" +
        "            <button onclick=\"cambiarEstadoCentral('ALL', 'BLOQUEAR')\" class=\"px-3 py-2 bg-amber-600 hover:bg-amber-700 text-white rounded-lg text-xs font-semibold transition flex items-center space-x-1.5\">\n" +
        "                <i class=\"fa-solid fa-ban\"></i>\n" +
        "                <span>Bloquear Todas (Out of Service)</span>\n" +
        "            </button>\n" +
        "            <button onclick=\"cambiarEstadoCentral('ALL', 'ACTIVAR')\" class=\"px-3 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-lg text-xs font-semibold transition flex items-center space-x-1.5\">\n" +
        "                <i class=\"fa-solid fa-check\"></i>\n" +
        "                <span>Activar Todas</span>\n" +
        "            </button>\n" +
        "            <button onclick=\"paradaEmergencia()\" class=\"px-4 py-2 bg-rose-600 hover:bg-rose-700 text-white rounded-lg text-sm font-semibold shadow-lg shadow-rose-600/30 transition flex items-center space-x-2\">\n" +
        "                <i class=\"fa-solid fa-triangle-exclamation\"></i>\n" +
        "                <span>Parada de Emergencia</span>\n" +
        "            </button>\n" +
        "        </div>\n" +
        "    </header>\n" +
        "\n" +
        "    <main class=\"max-w-7xl mx-auto p-6 space-y-6\">\n" +
        "\n" +
        "        <!-- Métricas Principales -->\n" +
        "        <div class=\"grid grid-cols-1 md:grid-cols-5 gap-4\">\n" +
        "            <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm\">\n" +
        "                <div class=\"flex items-center justify-between text-slate-400 text-xs font-medium mb-1\">\n" +
        "                    <span>Estaciones</span>\n" +
        "                    <i class=\"fa-solid fa-network-wired text-cyan-400\"></i>\n" +
        "                </div>\n" +
        "                <div id=\"stat-total\" class=\"text-2xl font-extrabold text-white\">7</div>\n" +
        "                <div class=\"text-[11px] text-slate-500 mt-1\">Red Municipal</div>\n" +
        "            </div>\n" +
        "            <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm\">\n" +
        "                <div class=\"flex items-center justify-between text-slate-400 text-xs font-medium mb-1\">\n" +
        "                    <span>Regando (Verde Parp.)</span>\n" +
        "                    <i class=\"fa-solid fa-water text-emerald-400 animate-bounce\"></i>\n" +
        "                </div>\n" +
        "                <div id=\"stat-regando\" class=\"text-2xl font-extrabold text-emerald-400\">0</div>\n" +
        "                <div class=\"text-[11px] text-slate-500 mt-1\">Suministrando agua</div>\n" +
        "            </div>\n" +
        "            <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm\">\n" +
        "                <div class=\"flex items-center justify-between text-slate-400 text-xs font-medium mb-1\">\n" +
        "                    <span>Disponibles (Verde)</span>\n" +
        "                    <i class=\"fa-solid fa-circle-check text-emerald-500\"></i>\n" +
        "                </div>\n" +
        "                <div id=\"stat-disponibles\" class=\"text-2xl font-extrabold text-emerald-500\">0</div>\n" +
        "                <div class=\"text-[11px] text-slate-500 mt-1\">A la espera de orden</div>\n" +
        "            </div>\n" +
        "            <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm\">\n" +
        "                <div class=\"flex items-center justify-between text-slate-400 text-xs font-medium mb-1\">\n" +
        "                    <span>Fugas (Rojo)</span>\n" +
        "                    <i class=\"fa-solid fa-triangle-exclamation text-rose-400\"></i>\n" +
        "                </div>\n" +
        "                <div id=\"stat-fugas\" class=\"text-2xl font-extrabold text-rose-400\">0</div>\n" +
        "                <div class=\"text-[11px] text-slate-500 mt-1\">Averías detectadas</div>\n" +
        "            </div>\n" +
        "            <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm\">\n" +
        "                <div class=\"flex items-center justify-between text-slate-400 text-xs font-medium mb-1\">\n" +
        "                    <span>Out of Service (Naranja)</span>\n" +
        "                    <i class=\"fa-solid fa-ban text-amber-400\"></i>\n" +
        "                </div>\n" +
        "                <div id=\"stat-bloqueadas\" class=\"text-2xl font-extrabold text-amber-400\">0</div>\n" +
        "                <div class=\"text-[11px] text-slate-500 mt-1\">Bloqueadas por Central</div>\n" +
        "            </div>\n" +
        "        </div>\n" +
        "\n" +
        "        <!-- SECCIÓN: MAPA INTERACTIVO DE LA RED DE RIEGO -->\n" +
        "        <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-sm\">\n" +
        "            <div class=\"flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 mb-4\">\n" +
        "                <div>\n" +
        "                    <h2 class=\"text-lg font-bold text-slate-200 flex items-center space-x-2\">\n" +
        "                        <i class=\"fa-solid fa-map-location-dot text-emerald-400\"></i>\n" +
        "                        <span>Mapa Topológico de la Red Hidráulica (Colores Oficiales)</span>\n" +
        "                    </h2>\n" +
        "                    <p class=\"text-xs text-slate-400\">Visualización de estados y distribución de agua en tiempo real</p>\n" +
        "                </div>\n" +
        "                <!-- Leyenda de colores Oficial del Enunciado -->\n" +
        "                <div class=\"flex flex-wrap items-center gap-3 text-xs bg-slate-950 px-3 py-2 rounded-xl border border-slate-800\">\n" +
        "                    <span class=\"flex items-center space-x-1.5 text-emerald-400 font-medium\">\n" +
        "                        <span class=\"w-2.5 h-2.5 rounded-full bg-emerald-500\"></span>\n" +
        "                        <span>Available / Disponible (Verde)</span>\n" +
        "                    </span>\n" +
        "                    <span class=\"flex items-center space-x-1.5 text-emerald-300 font-medium\">\n" +
        "                        <span class=\"w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping\"></span>\n" +
        "                        <span>Watering / Regando (Verde Parp.)</span>\n" +
        "                    </span>\n" +
        "                    <span class=\"flex items-center space-x-1.5 text-rose-400 font-medium\">\n" +
        "                        <span class=\"w-2.5 h-2.5 rounded-full bg-rose-500 animate-pulse\"></span>\n" +
        "                        <span>Leak / Fuga (Rojo)</span>\n" +
        "                    </span>\n" +
        "                    <span class=\"flex items-center space-x-1.5 text-amber-400 font-medium\">\n" +
        "                        <span class=\"w-2.5 h-2.5 rounded-full bg-amber-500\"></span>\n" +
        "                        <span>Out of Service (Naranja)</span>\n" +
        "                    </span>\n" +
        "                    <span class=\"flex items-center space-x-1.5 text-slate-400 font-medium\">\n" +
        "                        <span class=\"w-2.5 h-2.5 rounded-full bg-slate-600\"></span>\n" +
        "                        <span>Disconnected (Gris)</span>\n" +
        "                    </span>\n" +
        "                </div>\n" +
        "            </div>\n" +
        "\n" +
        "            <!-- Contenedor del Mapa SVG -->\n" +
        "            <div class=\"relative w-full h-80 bg-slate-950 rounded-xl overflow-hidden border border-slate-800/80 flex items-center justify-center\">\n" +
        "                <!-- SVG de tuberías interactivas -->\n" +
        "                <svg id=\"svg-mapa\" class=\"absolute inset-0 w-full h-full\" viewBox=\"0 0 1000 500\" preserveAspectRatio=\"xMidYMid meet\">\n" +
        "                    <defs>\n" +
        "                        <linearGradient id=\"grad-central\" x1=\"0%\" y1=\"0%\" x2=\"100%\" y2=\"100%\">\n" +
        "                            <stop offset=\"0%\" stop-color=\"#10b981\" />\n" +
        "                            <stop offset=\"100%\" stop-color=\"#059669\" />\n" +
        "                        </linearGradient>\n" +
        "                    </defs>\n" +
        "\n" +
        "                    <!-- Tuberías hacia cada una de las 7 estaciones -->\n" +
        "                    <g id=\"tubo-WS_01\"><path d=\"M 500 250 L 500 65\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "                    <g id=\"tubo-WS_02\"><path d=\"M 500 250 L 780 110\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "                    <g id=\"tubo-WS_03\"><path d=\"M 500 250 L 870 260\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "                    <g id=\"tubo-WS_04\"><path d=\"M 500 250 L 750 420\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "                    <g id=\"tubo-WS_05\"><path d=\"M 500 250 L 250 420\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "                    <g id=\"tubo-WS_06\"><path d=\"M 500 250 L 130 260\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "                    <g id=\"tubo-WS_07\"><path d=\"M 500 250 L 220 110\" stroke=\"#334155\" stroke-width=\"4\" fill=\"none\"/></g>\n" +
        "\n" +
        "                    <!-- Nodo Central WM_Central -->\n" +
        "                    <g transform=\"translate(500, 250)\">\n" +
        "                        <circle r=\"36\" fill=\"url(#grad-central)\" class=\"shadow-lg filter drop-shadow(0 0 12px rgba(16,185,129,0.5))\" />\n" +
        "                        <circle r=\"42\" fill=\"none\" stroke=\"#34d399\" stroke-width=\"1.5\" opacity=\"0.4\" class=\"animate-ping\" />\n" +
        "                        <text y=\"-5\" text-anchor=\"middle\" fill=\"#ffffff\" font-size=\"12\" font-weight=\"bold\" font-family=\"sans-serif\">HUB CENTRAL</text>\n" +
        "                        <text y=\"12\" text-anchor=\"middle\" fill=\"#d1fae5\" font-size=\"9\" font-family=\"sans-serif\">WM Central</text>\n" +
        "                    </g>\n" +
        "                </svg>\n" +
        "\n" +
        "                <!-- Nodos HTML superpuestos en las coordenadas exactas -->\n" +
        "                <div id=\"map-nodes-container\" class=\"absolute inset-0 pointer-events-none\">\n" +
        "                    <!-- Nodos generados reactivamente -->\n" +
        "                </div>\n" +
        "            </div>\n" +
        "        </div>\n" +
        "\n" +
        "        <!-- Grid de Estaciones y Gráfico -->\n" +
        "        <div class=\"grid grid-cols-1 lg:grid-cols-3 gap-6\">\n" +
        "\n" +
        "            <!-- Lista de Tarjetas de Estaciones (2 columnas en LG) -->\n" +
        "            <div class=\"lg:col-span-2 space-y-4\">\n" +
        "                <div class=\"flex items-center justify-between\">\n" +
        "                    <h2 class=\"text-lg font-bold text-slate-200 flex items-center space-x-2\">\n" +
        "                        <i class=\"fa-solid fa-satellite-dish text-emerald-400\"></i>\n" +
        "                        <span>Panel de Detalle de Estaciones</span>\n" +
        "                    </h2>\n" +
        "                    <span class=\"text-xs text-slate-500\">Actualización en tiempo real</span>\n" +
        "                </div>\n" +
        "\n" +
        "                <div id=\"grid-estaciones\" class=\"grid grid-cols-1 sm:grid-cols-2 gap-4\">\n" +
        "                    <!-- Tarjetas generadas dinámicamente -->\n" +
        "                </div>\n" +
        "            </div>\n" +
        "\n" +
        "            <!-- Gráfico y Consola de Eventos -->\n" +
        "            <div class=\"space-y-6\">\n" +
        "                <!-- Gráfico de Consumo -->\n" +
        "                <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm\">\n" +
        "                    <h3 class=\"text-sm font-bold text-slate-300 mb-4 flex items-center space-x-2\">\n" +
        "                        <i class=\"fa-solid fa-chart-line text-emerald-400\"></i>\n" +
        "                        <span>Caudal en Tiempo Real (Litros / Minuto)</span>\n" +
        "                    </h3>\n" +
        "                    <div class=\"h-56\">\n" +
        "                        <canvas id=\"chartCaudal\"></canvas>\n" +
        "                    </div>\n" +
        "                </div>\n" +
        "\n" +
        "                <!-- Feed de Logs / Eventos -->\n" +
        "                <div class=\"bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-sm\">\n" +
        "                    <div class=\"flex items-center justify-between mb-3\">\n" +
        "                        <h3 class=\"text-sm font-bold text-slate-300 flex items-center space-x-2\">\n" +
        "                            <i class=\"fa-solid fa-terminal text-emerald-400\"></i>\n" +
        "                            <span>Registro de Actividad y Eventos</span>\n" +
        "                        </h3>\n" +
        "                        <button onclick=\"limpiarLogs()\" class=\"text-xs text-slate-500 hover:text-slate-300\">Limpiar</button>\n" +
        "                    </div>\n" +
        "                    <div id=\"console-logs\" class=\"bg-slate-950 p-3 rounded-xl font-mono text-xs text-slate-400 h-48 overflow-y-auto space-y-1 border border-slate-800/80\">\n" +
        "                        <div>[00:00:00] Esperando conexión de eventos...</div>\n" +
        "                    </div>\n" +
        "                </div>\n" +
        "            </div>\n" +
        "\n" +
        "        </div>\n" +
        "\n" +
        "    </main>\n" +
        "\n" +
        "    <script>\n" +
        "        let chart = null;\n" +
        "\n" +
        "        const MAP_POSITIONS = {\n" +
        "            \"WS_01\": { x: 50, y: 13, name: \"P. Central Norte\" },\n" +
        "            \"WS_02\": { x: 78, y: 22, name: \"P. Central Sur\" },\n" +
        "            \"WS_03\": { x: 87, y: 52, name: \"Jardines del Prado\" },\n" +
        "            \"WS_04\": { x: 75, y: 84, name: \"Av. Palmeras\" },\n" +
        "            \"WS_05\": { x: 25, y: 84, name: \"Plaza Mayor\" },\n" +
        "            \"WS_06\": { x: 13, y: 52, name: \"Parque Fluvial\" },\n" +
        "            \"WS_07\": { x: 22, y: 22, name: \"Vivero Municipal\" }\n" +
        "        };\n" +
        "\n" +
        "        function initChart() {\n" +
        "            const ctx = document.getElementById('chartCaudal').getContext('2d');\n" +
        "            chart = new Chart(ctx, {\n" +
        "                type: 'bar',\n" +
        "                data: {\n" +
        "                    labels: [],\n" +
        "                    datasets: [{\n" +
        "                        label: 'Caudal (L/min)',\n" +
        "                        data: [],\n" +
        "                        backgroundColor: 'rgba(16, 185, 129, 0.65)',\n" +
        "                        borderColor: '#10b981',\n" +
        "                        borderWidth: 1.5,\n" +
        "                        borderRadius: 6\n" +
        "                    }]\n" +
        "                },\n" +
        "                options: {\n" +
        "                    responsive: true,\n" +
        "                    maintainAspectRatio: false,\n" +
        "                    scales: {\n" +
        "                        y: {\n" +
        "                            beginAtZero: true,\n" +
        "                            grid: { color: 'rgba(51, 65, 85, 0.4)' },\n" +
        "                            ticks: { color: '#94a3b8' }\n" +
        "                        },\n" +
        "                        x: {\n" +
        "                            grid: { display: false },\n" +
        "                            ticks: { color: '#94a3b8' }\n" +
        "                        }\n" +
        "                    },\n" +
        "                    plugins: {\n" +
        "                        legend: { display: false }\n" +
        "                    }\n" +
        "                }\n" +
        "            });\n" +
        "        }\n" +
        "\n" +
        "        async function actualizarDatos() {\n" +
        "            try {\n" +
        "                const resp = await fetch('/api/estaciones');\n" +
        "                const estaciones = await resp.json();\n" +
        "\n" +
        "                let regando = 0, disponibles = 0, fugas = 0, bloqueadas = 0;\n" +
        "                const grid = document.getElementById('grid-estaciones');\n" +
        "                const mapContainer = document.getElementById('map-nodes-container');\n" +
        "                \n" +
        "                grid.innerHTML = '';\n" +
        "                mapContainer.innerHTML = '';\n" +
        "\n" +
        "                const labels = [];\n" +
        "                const caudalesLMin = [];\n" +
        "\n" +
        "                estaciones.forEach(e => {\n" +
        "                    if (e.estado === 'REGANDO') regando++;\n" +
        "                    else if (e.estado === 'DISPONIBLE') disponibles++;\n" +
        "                    else if (e.estado === 'FUGA') fugas++;\n" +
        "                    else if (e.estado === 'FUERA_DE_SERVICIO') bloqueadas++;\n" +
        "\n" +
        "                    labels.push(e.idWS);\n" +
        "                    caudalesLMin.push(e.caudalLMin || 0);\n" +
        "\n" +
        "                    // --- COLORES OFICIALES SEGÚN ESPECIFICACIÓN PDF ---\n" +
        "                    let badgeClass = 'bg-slate-800 text-slate-400 border border-slate-700';\n" +
        "                    let cardBgClass = 'bg-slate-900 border-slate-800';\n" +
        "                    let icon = 'fa-power-off text-slate-500';\n" +
        "                    let mapColor = 'bg-slate-800 border-slate-600 text-slate-400';\n" +
        "                    let pipeStroke = '#334155';\n" +
        "                    let pipeClass = '';\n" +
        "                    let estadoLabel = e.estado;\n" +
        "\n" +
        "                    if (e.estado === 'DISPONIBLE') {\n" +
        "                        // VERDE\n" +
        "                        badgeClass = 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40';\n" +
        "                        cardBgClass = 'bg-emerald-950/20 border-emerald-500/40';\n" +
        "                        icon = 'fa-circle-check text-emerald-400';\n" +
        "                        mapColor = 'bg-emerald-600 border-emerald-300 text-white shadow-lg shadow-emerald-500/40';\n" +
        "                        pipeStroke = '#10b981';\n" +
        "                        estadoLabel = 'DISPONIBLE';\n" +
        "                    } else if (e.estado === 'REGANDO') {\n" +
        "                        // VERDE PARPADEANDO\n" +
        "                        badgeClass = 'bg-emerald-500 text-slate-950 font-extrabold border border-emerald-300 animate-pulse';\n" +
        "                        cardBgClass = 'animate-regando-card border-emerald-400 text-white';\n" +
        "                        icon = 'fa-water text-emerald-300 animate-pulse';\n" +
        "                        mapColor = 'bg-emerald-500 border-white text-slate-950 font-extrabold shadow-lg shadow-emerald-400/80 animate-ping';\n" +
        "                        pipeStroke = '#10b981';\n" +
        "                        pipeClass = 'pipe-flow';\n" +
        "                        estadoLabel = 'REGANDO (EN CURSO)';\n" +
        "                    } else if (e.estado === 'FUGA') {\n" +
        "                        // ROJO\n" +
        "                        badgeClass = 'bg-rose-600/30 text-rose-300 border border-rose-500 animate-fuga';\n" +
        "                        cardBgClass = 'bg-rose-950/30 border-rose-500/60 shadow-lg shadow-rose-500/20 animate-fuga';\n" +
        "                        icon = 'fa-triangle-exclamation text-rose-400';\n" +
        "                        mapColor = 'bg-rose-600 border-rose-300 text-white shadow-lg shadow-rose-500/60 animate-fuga';\n" +
        "                        pipeStroke = '#f43f5e';\n" +
        "                        estadoLabel = 'FUGA DETECTADA';\n" +
        "                    } else if (e.estado === 'FUERA_DE_SERVICIO') {\n" +
        "                        // NARANJA\n" +
        "                        badgeClass = 'bg-amber-600/30 text-amber-300 border border-amber-500';\n" +
        "                        cardBgClass = 'bg-amber-950/20 border-amber-500/40';\n" +
        "                        icon = 'fa-ban text-amber-400';\n" +
        "                        mapColor = 'bg-amber-600 border-amber-300 text-white shadow-lg shadow-amber-500/40';\n" +
        "                        pipeStroke = '#f59e0b';\n" +
        "                        estadoLabel = 'Out of Service (Fuera de Servicio)';\n" +
        "                    } else {\n" +
        "                        // GRIS (DESCONECTADA)\n" +
        "                        badgeClass = 'bg-slate-800 text-slate-400 border border-slate-700';\n" +
        "                        cardBgClass = 'bg-slate-900 border-slate-800 opacity-70';\n" +
        "                        icon = 'fa-power-off text-slate-500';\n" +
        "                        mapColor = 'bg-slate-800 border-slate-600 text-slate-400';\n" +
        "                        pipeStroke = '#334155';\n" +
        "                        estadoLabel = 'DESCONECTADA';\n" +
        "                    }\n" +
        "\n" +
        "                    // Actualizar tubería SVG\n" +
        "                    const tuboG = document.getElementById(`tubo-${e.idWS}`);\n" +
        "                    if (tuboG) {\n" +
        "                        const path = tuboG.querySelector('path');\n" +
        "                        if (path) {\n" +
        "                            path.setAttribute('stroke', pipeStroke);\n" +
        "                            path.setAttribute('class', pipeClass);\n" +
        "                            path.setAttribute('stroke-width', e.estado === 'REGANDO' ? '6' : '4');\n" +
        "                        }\n" +
        "                    }\n" +
        "\n" +
        "                    // Renderizar Nodo en el Mapa\n" +
        "                    const pos = MAP_POSITIONS[e.idWS] || { x: 50, y: 50, name: e.idWS };\n" +
        "                    const mapNode = document.createElement('div');\n" +
        "                    mapNode.className = `absolute transform -translate-x-1/2 -translate-y-1/2 pointer-events-auto cursor-pointer group`;\n" +
        "                    mapNode.style.left = `${pos.x}%`;\n" +
        "                    mapNode.style.top = `${pos.y}%`;\n" +
        "                    mapNode.onclick = () => {\n" +
        "                        if (e.estado === 'DISPONIBLE') iniciarRiegoPrompt(e.idWS);\n" +
        "                        else if (e.estado === 'REGANDO') detenerRiego(e.idWS);\n" +
        "                    };\n" +
        "\n" +
        "                    mapNode.innerHTML = `\n" +
        "                        <div class=\"flex flex-col items-center\">\n" +
        "                            <div class=\"w-10 h-10 rounded-full border-2 ${mapColor} flex items-center justify-center font-bold text-xs transition duration-300 transform group-hover:scale-110\">\n" +
        "                                <i class=\"fa-solid ${e.estado === 'REGANDO' ? 'fa-droplet animate-bounce' : (e.estado === 'FUGA' ? 'fa-triangle-exclamation' : (e.estado === 'FUERA_DE_SERVICIO' ? 'fa-ban' : 'fa-seedling'))}\"></i>\n" +
        "                            </div>\n" +
        "                            <span class=\"mt-1 px-2 py-0.5 rounded bg-slate-900/90 text-[10px] font-bold text-slate-200 border border-slate-700 whitespace-nowrap shadow\">\n" +
        "                                ${e.idWS}\n" +
        "                            </span>\n" +
        "                            ${e.estado === 'REGANDO' ? `<span class=\"text-[9px] font-bold text-emerald-400 bg-slate-950 px-1.5 rounded\">${e.caudalLMin.toFixed(0)} L/min</span>` : ''}\n" +
        "                        </div>\n" +
        "                    `;\n" +
        "                    mapContainer.appendChild(mapNode);\n" +
        "\n" +
        "                    // Renderizar Tarjeta en la cuadrícula con los datos exactos del PDF\n" +
        "                    const card = document.createElement('div');\n" +
        "                    card.className = `border rounded-2xl p-5 space-y-4 transition duration-300 ${cardBgClass}`;\n" +
        "                    card.innerHTML = `\n" +
        "                        <div class=\"flex items-center justify-between\">\n" +
        "                            <div class=\"flex items-center space-x-2\">\n" +
        "                                <i class=\"fa-solid ${icon}\"></i>\n" +
        "                                <span class=\"font-bold text-lg text-white\">${e.idWS}</span>\n" +
        "                            </div>\n" +
        "                            <span class=\"px-2.5 py-0.5 rounded-full text-xs font-semibold ${badgeClass}\">\n" +
        "                                ${estadoLabel}\n" +
        "                            </span>\n" +
        "                        </div>\n" +
        "\n" +
        "                        <div class=\"text-xs text-slate-300 space-y-1\">\n" +
        "                            <div class=\"flex justify-between\">\n" +
        "                                <span class=\"text-slate-400\">Ubicación:</span>\n" +
        "                                <span class=\"font-medium text-slate-100\">${e.ubicacion}</span>\n" +
        "                            </div>\n" +
        "                            <div class=\"flex justify-between\">\n" +
        "                                <span class=\"text-slate-400\">Id Operario:</span>\n" +
        "                                <span class=\"font-medium text-emerald-400\">${e.operador || '-'}</span>\n" +
        "                            </div>\n" +
        "                        </div>\n" +
        "\n" +
        "                        <div class=\"grid grid-cols-2 gap-2 bg-slate-950/80 p-3 rounded-xl border border-slate-800\">\n" +
        "                            <div>\n" +
        "                                <span class=\"text-[10px] text-slate-400 uppercase font-semibold\">Caudal Actual</span>\n" +
        "                                <div class=\"text-base font-bold text-emerald-400\">${e.caudalLMin.toFixed(1)} <span class=\"text-xs font-normal text-slate-400\">L/min</span></div>\n" +
        "                            </div>\n" +
        "                            <div>\n" +
        "                                <span class=\"text-[10px] text-slate-400 uppercase font-semibold\">Volumen Acumulado</span>\n" +
        "                                <div class=\"text-base font-bold text-cyan-400\">${e.volumen.toFixed(1)} <span class=\"text-xs font-normal text-slate-400\">Litros</span></div>\n" +
        "                            </div>\n" +
        "                        </div>\n" +
        "\n" +
        "                        <div class=\"flex flex-wrap gap-2 pt-1\">\n" +
        "                            ${e.estado === 'DISPONIBLE' ? `\n" +
        "                                <button onclick=\"iniciarRiegoPrompt('${e.idWS}')\" class=\"flex-1 py-1.5 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-xs font-bold transition flex items-center justify-center space-x-1 shadow\">\n" +
        "                                    <i class=\"fa-solid fa-play text-[10px]\"></i>\n" +
        "                                    <span>Regar</span>\n" +
        "                                </button>\n" +
        "                                <button onclick=\"cambiarEstadoCentral('${e.idWS}', 'BLOQUEAR')\" class=\"py-1.5 px-3 bg-amber-600 hover:bg-amber-700 text-white rounded-lg text-xs font-bold transition\">\n" +
        "                                    <span>Bloquear</span>\n" +
        "                                </button>\n" +
        "                            ` : ''}\n" +
        "                            ${e.estado === 'REGANDO' ? `\n" +
        "                                <button onclick=\"detenerRiego('${e.idWS}')\" class=\"flex-1 py-1.5 bg-rose-600 hover:bg-rose-700 text-white rounded-lg text-xs font-bold transition flex items-center justify-center space-x-1 shadow\">\n" +
        "                                    <i class=\"fa-solid fa-stop text-[10px]\"></i>\n" +
        "                                    <span>Detener Riego</span>\n" +
        "                                </button>\n" +
        "                            ` : ''}\n" +
        "                            ${e.estado === 'FUERA_DE_SERVICIO' ? `\n" +
        "                                <button onclick=\"cambiarEstadoCentral('${e.idWS}', 'ACTIVAR')\" class=\"flex-1 py-1.5 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-xs font-bold transition\">\n" +
        "                                    <span>Activar Estación</span>\n" +
        "                                </button>\n" +
        "                            ` : ''}\n" +
        "                            ${e.estado === 'FUGA' ? `\n" +
        "                                <button onclick=\"cambiarEstadoCentral('${e.idWS}', 'ACTIVAR')\" class=\"flex-1 py-1.5 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-xs font-bold transition\">\n" +
        "                                    <span>Resolver Avería (OK)</span>\n" +
        "                                </button>\n" +
        "                            ` : ''}\n" +
        "                        </div>\n" +
        "                    `;\n" +
        "                    grid.appendChild(card);\n" +
        "                });\n" +
        "\n" +
        "                document.getElementById('stat-total').innerText = estaciones.length;\n" +
        "                document.getElementById('stat-regando').innerText = regando;\n" +
        "                document.getElementById('stat-disponibles').innerText = disponibles;\n" +
        "                document.getElementById('stat-fugas').innerText = fugas;\n" +
        "                document.getElementById('stat-bloqueadas').innerText = bloqueadas;\n" +
        "\n" +
        "                if (chart) {\n" +
        "                    chart.data.labels = labels;\n" +
        "                    chart.data.datasets[0].data = caudalesLMin;\n" +
        "                    chart.update('none');\n" +
        "                }\n" +
        "\n" +
        "                const respLogs = await fetch('/api/logs');\n" +
        "                const logs = await respLogs.json();\n" +
        "                const logContainer = document.getElementById('console-logs');\n" +
        "                if (logs.length > 0) {\n" +
        "                    logContainer.innerHTML = logs.map(l => `<div>${l}</div>`).join('');\n" +
        "                    logContainer.scrollTop = logContainer.scrollHeight;\n" +
        "                }\n" +
        "\n" +
        "            } catch (err) {\n" +
        "                console.error(\"Error al refrescar dashboard:\", err);\n" +
        "            }\n" +
        "        }\n" +
        "\n" +
        "        async function iniciarRiegoPrompt(idWS) {\n" +
        "            const duracion = prompt(`Introduce duración de riego en segundos para ${idWS}:`, \"10\");\n" +
        "            if (!duracion) return;\n" +
        "            try {\n" +
        "                const resp = await fetch('/api/riego', {\n" +
        "                    method: 'POST',\n" +
        "                    body: `idWS=${encodeURIComponent(idWS)}&accion=START&duracion=${encodeURIComponent(duracion)}`\n" +
        "                });\n" +
        "                const res = await resp.json();\n" +
        "                if (!res.success) alert(res.message);\n" +
        "                actualizarDatos();\n" +
        "            } catch (err) {\n" +
        "                alert(\"Error al enviar orden: \" + err);\n" +
        "            }\n" +
        "        }\n" +
        "\n" +
        "        async function detenerRiego(idWS) {\n" +
        "            try {\n" +
        "                await fetch('/api/riego', {\n" +
        "                    method: 'POST',\n" +
        "                    body: `idWS=${encodeURIComponent(idWS)}&accion=STOP`\n" +
        "                });\n" +
        "                actualizarDatos();\n" +
        "            } catch (err) {\n" +
        "                alert(\"Error al detener riego: \" + err);\n" +
        "            }\n" +
        "        }\n" +
        "\n" +
        "        async function cambiarEstadoCentral(idWS, accion) {\n" +
        "            try {\n" +
        "                const resp = await fetch('/api/estado', {\n" +
        "                    method: 'POST',\n" +
        "                    body: `idWS=${encodeURIComponent(idWS)}&accion=${encodeURIComponent(accion)}`\n" +
        "                });\n" +
        "                const res = await resp.json();\n" +
        "                if (!res.success) alert(res.message);\n" +
        "                actualizarDatos();\n" +
        "            } catch (err) {\n" +
        "                alert(\"Error al cambiar estado: \" + err);\n" +
        "            }\n" +
        "        }\n" +
        "\n" +
        "        async function paradaEmergencia() {\n" +
        "            if (confirm(\"¿Confirmas la parada de emergencia de TODAS las estaciones de riego?\")) {\n" +
        "                await fetch('/api/emergencia', { method: 'POST' });\n" +
        "                actualizarDatos();\n" +
        "            }\n" +
        "        }\n" +
        "\n" +
        "        function limpiarLogs() {\n" +
        "            document.getElementById('console-logs').innerHTML = '';\n" +
        "        }\n" +
        "\n" +
        "        window.onload = () => {\n" +
        "            initChart();\n" +
        "            actualizarDatos();\n" +
        "            setInterval(actualizarDatos, 1000);\n" +
        "        };\n" +
        "    </script>\n" +
        "</body>\n" +
        "</html>\n" +
        "    \n";
}
