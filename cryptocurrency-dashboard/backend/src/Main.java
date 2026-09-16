import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Crypto Terminal backend.
 * - HTTP on 8080: static frontend + /api/crypto + /api/health
 * - Raw WebSocket broadcast on 8081 (RFC6455, JDK only)
 * - Live source: Binance WS (!ticker@arr) via JDK HttpClient WebSocket,
 *   fallback to Binance REST poll every 2s, fallback to local simulation.
 */
public class Main {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Path FRONTEND_BASE = resolveFrontendBase();
    private static final int HTTP_PORT = 8080;
    private static final int WS_PORT = 8081;

    private static final String BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!ticker@arr";
    private static final String BINANCE_REST = "https://api.binance.com/api/v3/ticker/24hr";

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // short -> pair
    private static final Map<String, String> PAIRS = Map.of(
            "BTC", "BTCUSDT",
            "ETH", "ETHUSDT",
            "SOL", "SOLUSDT",
            "ADA", "ADAUSDT",
            "MATIC", "MATICUSDT",
            "XRP", "XRPUSDT");

    private static final Map<String, String> NAMES = Map.of(
            "BTC", "Bitcoin",
            "ETH", "Ethereum",
            "SOL", "Solana",
            "ADA", "Cardano",
            "MATIC", "Polygon",
            "XRP", "Ripple");

    private static final Map<String, String> IDS = Map.of(
            "BTC", "bitcoin",
            "ETH", "ethereum",
            "SOL", "solana",
            "ADA", "cardano",
            "MATIC", "polygon",
            "XRP", "ripple");

    private static final Map<String, Double> BASE = Map.of(
            "BTC", 67543.21,
            "ETH", 3456.78,
            "SOL", 145.67,
            "ADA", 0.4521,
            "MATIC", 0.8912,
            "XRP", 0.6234);

    private static final Map<String, Long> SUPPLY = Map.of(
            "BTC", 19_700_000L,
            "ETH", 120_200_000L,
            "SOL", 460_000_000L,
            "ADA", 35_600_000_000L,
            "MATIC", 9_900_000_000L,
            "XRP", 55_000_000_000L);

    private static final ConcurrentHashMap<String, AssetState> STORE = new ConcurrentHashMap<>();
    private static volatile long lastBinanceMsgMs = 0;
    private static volatile boolean binanceWsLive = false;
    private static final long START_MS = System.currentTimeMillis();
    private static final Random RAND = new Random();

    private static WsBroadcaster WS;

    // ---------------- Asset ----------------
    static class AssetState {
        final String sym;      // BTC
        final String pair;     // BTCUSDT
        final String name;
        final String id;
        volatile double price;
        volatile double open;
        volatile double high;
        volatile double low;
        volatile double changePct;
        volatile double quoteVolume;
        final ConcurrentLinkedDeque<Double> history = new ConcurrentLinkedDeque<>();

        AssetState(String sym) {
            this.sym = sym;
            this.pair = PAIRS.get(sym);
            this.name = NAMES.get(sym);
            this.id = IDS.get(sym);
            double b = BASE.get(sym);
            this.price = b;
            this.open = b;
            this.high = b * 1.004;
            this.low = b * 0.996;
            this.changePct = 0.0;
            this.quoteVolume = b * 1_000_000;
            for (int i = 0; i < 40; i++) {
                history.add(b * (1 + (RAND.nextDouble() - 0.5) * 0.004));
            }
            history.add(b);
        }

        synchronized void applyLive(double last, double high, double low, double chgPct, double qvol) {
            if (last > 0) this.price = last;
            if (high > 0) this.high = high;
            if (low > 0) this.low = low;
            this.changePct = chgPct;
            if (qvol > 0) this.quoteVolume = qvol;
            push(price);
        }

        synchronized void simulateTick() {
            double drift = (RAND.nextDouble() - 0.5) * 0.0022;
            price = Math.max(price * (1 + drift), 0.0001);
            if (price > high) high = price;
            if (price < low) low = price;
            if (open != 0) changePct = (price - open) / open * 100.0;
            push(price);
        }

        private void push(double v) {
            history.add(v);
            while (history.size() > 80) history.pollFirst();
        }

        Map<String, Object> snapshot() {
            Map<String, Object> m = new LinkedHashMap<>();
            long mcap = (long) (price * SUPPLY.getOrDefault(sym, 1_000_000L));
            m.put("id", id);
            m.put("symbol", sym.toLowerCase());
            m.put("name", name);
            m.put("pair", pair);
            m.put("price", price);
            m.put("change24h", round2(changePct));
            m.put("current_price", price);
            m.put("price_change_percentage_24h", round2(changePct));
            m.put("high_24h", high);
            m.put("low_24h", low);
            m.put("market_cap", mcap);
            m.put("volume", quoteVolume);
            m.put("ts", System.currentTimeMillis());
            // last 40 points for sparkline seed
            List<Double> hist = new ArrayList<>(history);
            int from = Math.max(0, hist.size() - 40);
            m.put("history", hist.subList(from, hist.size()));
            return m;
        }
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static Path resolveFrontendBase() {
        String[] candidates = {
                "../frontend", "frontend",
                "/home/codespace/iv-surface/frontend"
        };
        for (String c : candidates) {
            Path p = Paths.get(c).toAbsolutePath().normalize();
            if (Files.isDirectory(p) && Files.exists(p.resolve("index.html"))) return p;
        }
        return Paths.get(System.getProperty("user.dir"), "..", "frontend").toAbsolutePath().normalize();
    }

    // ---------------- main ----------------
    public static void main(String[] args) throws Exception {
        for (String s : PAIRS.keySet()) STORE.put(s, new AssetState(s));

        WS = new WsBroadcaster(WS_PORT);
        WS.start();

        ScheduledExecutorService sched = Executors.newScheduledThreadPool(4);
        // Binance WS (live). JDK built-in WebSocket client.
        connectBinanceWs(sched);
        // REST fallback every 2s
        sched.scheduleAtFixedRate(Main::pollBinanceRest, 2, 2, TimeUnit.SECONDS);
        // simulation when stale
        sched.scheduleAtFixedRate(() -> {
            if (System.currentTimeMillis() - lastBinanceMsgMs > 5000) {
                for (AssetState a : STORE.values()) a.simulateTick();
            }
        }, 0, 700, TimeUnit.MILLISECONDS);
        // broadcast to WS clients ~ every 900ms
        sched.scheduleAtFixedRate(() -> {
            try {
                WS.broadcast(snapshotJson());
            } catch (Exception e) {
                System.err.println("broadcast failed: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);

        HttpServer server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
        server.createContext("/api/crypto", new CryptoHandler());
        server.createContext("/api/health", new HealthHandler());
        server.createContext("/", new StaticFileHandler());
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();

        System.out.println("Serving frontend from: " + FRONTEND_BASE);
        System.out.println("HTTP  http://localhost:" + HTTP_PORT);
        System.out.println("WS    ws://localhost:" + WS_PORT);
        System.out.println("API   http://localhost:" + HTTP_PORT + "/api/crypto");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { WS.stop(); } catch (Exception ignored) {}
            sched.shutdownNow();
            server.stop(0);
        }));
    }

    private static String snapshotJson() {
        try {
            List<Map<String, Object>> list = new ArrayList<>();
            // fixed order BTC ETH SOL ADA MATIC XRP
            for (String s : List.of("BTC", "ETH", "SOL", "ADA", "MATIC", "XRP")) {
                AssetState a = STORE.get(s);
                if (a != null) list.add(a.snapshot());
            }
            return MAPPER.writeValueAsString(list);
        } catch (Exception e) {
            return "[]";
        }
    }

    // ---------------- Binance live ----------------
    private static void connectBinanceWs(ScheduledExecutorService sched) {
        try {
            HTTP.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(8))
                    .buildAsync(URI.create(BINANCE_WS_URL), new WebSocket.Listener() {
                        private final StringBuilder buf = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket ws) {
                            binanceWsLive = true;
                            System.out.println("Binance WS connected");
                            ws.request(1);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                            buf.append(data);
                            if (last) {
                                String msg = buf.toString();
                                buf.setLength(0);
                                handleBinanceArr(msg);
                            }
                            ws.request(1);
                            return null;
                        }

                        @Override
                        public void onError(WebSocket ws, Throwable err) {
                            binanceWsLive = false;
                            System.err.println("Binance WS error: " + err.getMessage() + " (REST fallback active)");
                            sched.schedule(() -> connectBinanceWs(sched), 5, TimeUnit.SECONDS);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                            binanceWsLive = false;
                            System.out.println("Binance WS closed, reconnecting in 5s");
                            sched.schedule(() -> connectBinanceWs(sched), 5, TimeUnit.SECONDS);
                            return null;
                        }
                    });
        } catch (Exception e) {
            System.err.println("Binance WS connect failed, using REST/simulation: " + e.getMessage());
            sched.schedule(() -> connectBinanceWs(sched), 5, TimeUnit.SECONDS);
        }
    }

    private static void handleBinanceArr(String msg) {
        try {
            JsonNode root = MAPPER.readTree(msg);
            ArrayNode arr;
            if (root.isArray()) {
                arr = (ArrayNode) root;
            } else if (root.has("data") && root.get("data").isArray()) {
                arr = (ArrayNode) root.get("data");
            } else {
                return;
            }
            boolean touched = false;
            for (JsonNode n : arr) {
                String sym = n.path("s").asText(""); // e.g. BTCUSDT
                if (sym.isEmpty() || !sym.endsWith("USDT")) continue;
                String base = sym.substring(0, sym.length() - 4);
                // MATIC special-case: Binance may list MATICUSDT still
                AssetState a = STORE.get(base);
                if (a == null) continue;
                double last = parseD(n, "c");
                double high = parseD(n, "h");
                double low = parseD(n, "l");
                double chg = parseD(n, "P");
                double qv = parseD(n, "q");
                a.applyLive(last, high, low, chg, qv);
                touched = true;
            }
            if (touched) lastBinanceMsgMs = System.currentTimeMillis();
        } catch (Exception ignored) {
        }
    }

    private static double parseD(JsonNode n, String f) {
        try {
            JsonNode v = n.get(f);
            if (v == null || v.isNull()) return 0;
            if (v.isNumber()) return v.asDouble();
            return Double.parseDouble(v.asText("0"));
        } catch (Exception e) {
            return 0;
        }
    }

    private static void pollBinanceRest() {
        // Skip if WS is streaming fresh data (<4s old)
        if (binanceWsLive && System.currentTimeMillis() - lastBinanceMsgMs < 4000) return;
        try {
            String symbols = "[\"BTCUSDT\",\"ETHUSDT\",\"SOLUSDT\",\"ADAUSDT\",\"MATICUSDT\",\"XRPUSDT\"]";
            String url = BINANCE_REST + "?symbols=" + URLEncoder.encode(symbols, StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5)).GET().build();
            HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return;
            JsonNode arr = MAPPER.readTree(res.body());
            if (!arr.isArray()) return;
            boolean touched = false;
            for (JsonNode n : arr) {
                String sym = n.path("symbol").asText("");
                if (sym.isEmpty()) continue;
                String base = sym.replace("USDT", "");
                AssetState a = STORE.get(base);
                if (a == null) continue;
                double last = parseD(n, "lastPrice");
                double high = parseD(n, "highPrice");
                double low = parseD(n, "lowPrice");
                double chg = parseD(n, "priceChangePercent");
                double qv = parseD(n, "quoteVolume");
                a.applyLive(last, high, low, chg, qv);
                touched = true;
            }
            if (touched) {
                lastBinanceMsgMs = System.currentTimeMillis();
            }
        } catch (Exception e) {
            // offline / restricted env -> simulation keeps running
        }
    }

    // ---------------- HTTP handlers ----------------
    static class CryptoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
                ex.sendResponseHeaders(204, -1);
                ex.close();
                return;
            }
            if (!"GET".equals(ex.getRequestMethod())) {
                ex.sendResponseHeaders(405, -1);
                ex.close();
                return;
            }
            byte[] json = snapshotJson().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            ex.sendResponseHeaders(200, json.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(json);
            }
        }
    }

    static class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            try {
                Map<String, Object> h = new LinkedHashMap<>();
                h.put("status", "LIVE");
                h.put("uptimeSec", (System.currentTimeMillis() - START_MS) / 1000);
                h.put("wsPort", WS_PORT);
                h.put("wsClients", WS == null ? 0 : WS.clientCount());
                h.put("binanceWsLive", binanceWsLive);
                h.put("lastBinanceMsgAgeMs", System.currentTimeMillis() - lastBinanceMsgMs);
                h.put("time", Instant.now().toString());
                byte[] b = MAPPER.writeValueAsString(h).getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "application/json");
                ex.sendResponseHeaders(200, b.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(b);
                }
            } finally {
                ex.close();
            }
        }
    }

    static class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            if (!"GET".equals(ex.getRequestMethod())) {
                ex.sendResponseHeaders(405, -1);
                ex.close();
                return;
            }
            String path = ex.getRequestURI().getPath();
            if (path.equals("/")) path = "/index.html";
            Path resolved = FRONTEND_BASE.resolve(path.substring(1)).normalize();
            if (!resolved.startsWith(FRONTEND_BASE) || !Files.exists(resolved) || Files.isDirectory(resolved)) {
                byte[] nf = "404 Not Found".getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(404, nf.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(nf);
                }
                return;
            }
            ex.getResponseHeaders().set("Content-Type", mime(resolved.toString()));
            byte[] content = Files.readAllBytes(resolved);
            ex.sendResponseHeaders(200, content.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(content);
            }
        }

        private String mime(String f) {
            if (f.endsWith(".html")) return "text/html; charset=utf-8";
            if (f.endsWith(".css")) return "text/css; charset=utf-8";
            if (f.endsWith(".js")) return "application/javascript; charset=utf-8";
            if (f.endsWith(".json")) return "application/json";
            return "application/octet-stream";
        }
    }

    // ---------------- Minimal WebSocket broadcast server (RFC6455) ----------------
    static class WsBroadcaster {
        private final int port;
        private ServerSocket serverSocket;
        private final CopyOnWriteArraySet<WsConn> clients = new CopyOnWriteArraySet<>();
        private volatile boolean running = false;

        WsBroadcaster(int port) {
            this.port = port;
        }

        void start() throws IOException {
            serverSocket = new ServerSocket(port);
            running = true;
            Thread accept = new Thread(this::acceptLoop, "ws-accept");
            accept.setDaemon(true);
            accept.start();
            System.out.println("WS broadcaster on port " + port);
        }

        void stop() throws IOException {
            running = false;
            for (WsConn c : clients) c.closeQuietly();
            if (serverSocket != null) serverSocket.close();
        }

        int clientCount() {
            return clients.size();
        }

        void broadcast(String text) {
            if (clients.isEmpty()) return;
            byte[] frame = encodeText(text);
            for (WsConn c : clients) {
                try {
                    c.send(frame);
                } catch (IOException e) {
                    clients.remove(c);
                    c.closeQuietly();
                }
            }
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket s = serverSocket.accept();
                    s.setTcpNoDelay(true);
                    Thread t = new Thread(() -> handleClient(s), "ws-client");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    if (running) System.err.println("WS accept error: " + e.getMessage());
                }
            }
        }

        private void handleClient(Socket socket) {
            WsConn conn = null;
            try {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                String line;
                String key = null;
                // read handshake headers
                while ((line = reader.readLine()) != null && !line.isEmpty()) {
                    String low = line.toLowerCase();
                    if (low.startsWith("sec-websocket-key:")) {
                        key = line.substring(line.indexOf(':') + 1).trim();
                    }
                }
                if (key == null) {
                    socket.close();
                    return;
                }
                String accept = sha1Base64(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
                String resp = "HTTP/1.1 101 Switching Protocols\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
                out.write(resp.getBytes(StandardCharsets.UTF_8));
                out.flush();

                conn = new WsConn(socket, out);
                clients.add(conn);

                // read loop: handle ping/close so proxies don't kill idle conns
                while (running && !socket.isClosed()) {
                    Frame f = readFrame(in);
                    if (f == null) break;
                    if (f.opcode == 0x8) { // close
                        conn.send(encodeClose());
                        break;
                    } else if (f.opcode == 0x9) { // ping -> pong
                        conn.send(encodePong(f.payload));
                    }
                    // text/binary from client ignored (could support SUB commands)
                }
            } catch (Exception ignored) {
            } finally {
                if (conn != null) clients.remove(conn);
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }

        static class Frame {
            int opcode;
            byte[] payload;
        }

        private Frame readFrame(InputStream in) throws IOException {
            int b0 = in.read();
            if (b0 == -1) return null;
            int b1 = in.read();
            if (b1 == -1) return null;
            int opcode = b0 & 0x0F;
            boolean masked = (b1 & 0x80) != 0;
            long len = b1 & 0x7F;
            if (len == 126) {
                int h1 = in.read(), h2 = in.read();
                if (h1 == -1 || h2 == -1) return null;
                len = ((h1 << 8) | h2) & 0xFFFFL;
            } else if (len == 127) {
                byte[] ext = in.readNBytes(8);
                if (ext.length < 8) return null;
                len = 0;
                for (byte b : ext) len = (len << 8) | (b & 0xFF);
            }
            byte[] mask = new byte[4];
            if (masked) {
                int r = in.readNBytes(mask, 0, 4);
                if (r < 4) return null;
            }
            if (len > 8 * 1024 * 1024) return null; // sanity cap
            byte[] payload = in.readNBytes((int) len);
            if (masked) {
                for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
            }
            Frame f = new Frame();
            f.opcode = opcode;
            f.payload = payload;
            return f;
        }

        private static byte[] encodeText(String s) {
            byte[] data = s.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(0x81);
            writeLen(bos, data.length);
            bos.write(data, 0, data.length);
            return bos.toByteArray();
        }

        private static byte[] encodePong(byte[] data) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(0x8A);
            writeLen(bos, data.length);
            bos.write(data, 0, data.length);
            return bos.toByteArray();
        }

        private static byte[] encodeClose() {
            return new byte[]{(byte) 0x88, 0x00};
        }

        private static void writeLen(ByteArrayOutputStream bos, long len) {
            if (len < 126) {
                bos.write((int) len);
            } else if (len < 65536) {
                bos.write(126);
                bos.write((int) ((len >> 8) & 0xFF));
                bos.write((int) (len & 0xFF));
            } else {
                bos.write(127);
                for (int i = 7; i >= 0; i--) bos.write((int) ((len >> (8 * i)) & 0xFF));
            }
        }

        private static String sha1Base64(String s) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
                return Base64.getEncoder().encodeToString(d);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        static class WsConn {
            final Socket socket;
            final OutputStream out;

            WsConn(Socket socket, OutputStream out) {
                this.socket = socket;
                this.out = out;
            }

            synchronized void send(byte[] frame) throws IOException {
                out.write(frame);
                out.flush();
            }

            void closeQuietly() {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
