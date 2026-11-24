package lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Polymarket Research Lab.
 *
 * Single-file Java application:
 * - Public Polymarket market-data collection (no credentials required)
 * - SQLite persistence
 * - Historical price backfill and live order-book polling
 * - Five research modules
 * - Basic parameter search / walk-forward evaluation
 * - Paper-only signals (no order placement code exists)
 * - Small JSON API and static dashboard server
 *
 * Important: historical price candles are not historical order books. A signal
 * based on candles is a research observation, not proof that a trade could fill.
 */
public final class Main {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String GAMMA = env("GAMMA_URL", "https://gamma-api.polymarket.com");
    private static final String CLOB = env("CLOB_URL", "https://clob.polymarket.com");
    private static final String DATA_API = env("DATA_API_URL", "https://data-api.polymarket.com");
    private static final Path DATA_DIR = Path.of(env("DATA_DIR", "/app/data"));
    private static final Path DB_FILE = DATA_DIR.resolve("lab.db");
    private static final int PORT = Integer.parseInt(env("PORT", "8080"));
    private static final int MARKET_LIMIT = Integer.parseInt(env("MARKET_LIMIT", "250"));
    private static final int HISTORY_DAYS = Integer.parseInt(env("HISTORY_DAYS", "30"));
    private static final int MAX_HISTORY_TOKENS_PER_CYCLE =
            Integer.parseInt(env("MAX_HISTORY_TOKENS_PER_CYCLE", "60"));
    private static final int WORKERS = Integer.parseInt(env("WORKERS", "4"));
    private static final long BOOK_POLL_SECONDS = Long.parseLong(env("BOOK_POLL_SECONDS", "45"));
    private static final long MARKET_REFRESH_MINUTES = Long.parseLong(env("MARKET_REFRESH_MINUTES", "15"));
    private static final long HISTORY_REFRESH_HOURS = Long.parseLong(env("HISTORY_REFRESH_HOURS", "6"));

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final ExecutorService HTTP_POOL = Executors.newFixedThreadPool(
            Math.max(2, Math.min(8, WORKERS)), namedFactory("http"));
    private static final ExecutorService BACKTEST_POOL = Executors.newFixedThreadPool(
            Math.max(1, WORKERS), namedFactory("research"));
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(3, namedFactory("scheduler"));
    private static final AtomicBoolean STOPPING = new AtomicBoolean(false);
    private static final AtomicLong lastMarketRefresh = new AtomicLong();
    private static final AtomicLong lastHistoryRefresh = new AtomicLong();
    private static final Map<String, Book> BOOK_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, Market> MARKETS = new ConcurrentHashMap<>();
    private static final Deque<String> RECENT_LOGS = new ConcurrentLinkedDeque<>();
    private static volatile String systemStatus = "STARTING";
    private static volatile long startedAt = System.currentTimeMillis();

    private static final List<String> STRATEGY_NAMES = List.of(
            "Nested-market relationships",
            "YES/NO parity",
            "Short-term reversal",
            "Resolution countdown",
            "Cross-market lead-lag"
    );

    public static void main(String[] args) throws Exception {
        Files.createDirectories(DATA_DIR);
        Database.init();
        log("Starting Polymarket Research Lab; database=" + DB_FILE);
        startWebServer();
        systemStatus = "ONLINE";

        // Run an initial market refresh immediately, then keep collection independent
        // from backtests so slow historical calls cannot block current book capture.
        SCHEDULER.execute(() -> safeRun("initial market refresh", Main::refreshMarkets));
        SCHEDULER.scheduleWithFixedDelay(
                () -> safeRun("market refresh", Main::refreshMarkets),
                MARKET_REFRESH_MINUTES, MARKET_REFRESH_MINUTES, TimeUnit.MINUTES);
        SCHEDULER.scheduleWithFixedDelay(
                () -> safeRun("order-book polling", Main::pollBooks),
                10, BOOK_POLL_SECONDS, TimeUnit.SECONDS);
        SCHEDULER.scheduleWithFixedDelay(
                () -> safeRun("historical backfill", Main::backfillHistory),
                30, HISTORY_REFRESH_HOURS, TimeUnit.HOURS);
        SCHEDULER.scheduleWithFixedDelay(
                () -> safeRun("strategy scan", Main::runStrategyScans),
                20, 60, TimeUnit.SECONDS);
        SCHEDULER.scheduleWithFixedDelay(
                () -> safeRun("optimizer", Main::optimize),
                2, 30, TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdown, "shutdown"));
        log("Application ready. Dashboard: http://0.0.0.0:" + PORT);
    }

    // ---------------------------------------------------------------------
    // Collection
    // ---------------------------------------------------------------------

    private static void refreshMarkets() {
        long now = System.currentTimeMillis();
        if (now - lastMarketRefresh.get() < 60_000) return;
        lastMarketRefresh.set(now);
        int stored = 0;
        try {
            // Gamma's public markets endpoint supports active/closed filtering and offset pagination.
            for (int offset = 0; offset < MARKET_LIMIT; offset += 100) {
                String url = GAMMA + "/markets?active=true&closed=false&limit=100&offset=" + offset;
                JsonNode response = getJson(url);
                if (!response.isArray() || response.isEmpty()) break;
                for (JsonNode node : response) {
                    Market market = parseMarket(node);
                    if (market == null || market.tokens.isEmpty()) continue;
                    MARKETS.put(market.id, market);
                    Database.saveMarket(market);
                    stored++;
                }
                if (response.size() < 100) break;
            }
            log("Market refresh complete: " + stored + " active markets indexed.");
        } catch (Exception e) {
            log("Market refresh failed: " + shortError(e));
        }
    }

    private static Market parseMarket(JsonNode n) {
        String id = text(n, "conditionId", text(n, "id", ""));
        if (id.isBlank()) return null;
        String question = text(n, "question", text(n, "title", "Untitled market"));
        String slug = text(n, "slug", "");
        String endDate = text(n, "endDate", "");
        double volume = number(n, "volumeNum", number(n, "volume", 0));
        List<String> outcomes = parseStringArray(n.get("outcomes"));
        List<String> tokenIds = parseStringArray(n.get("clobTokenIds"));
        List<Token> tokens = new ArrayList<>();
        for (int i = 0; i < Math.min(outcomes.size(), tokenIds.size()); i++) {
            tokens.add(new Token(outcomes.get(i), tokenIds.get(i)));
        }
        return new Market(id, question, slug, endDate, volume, tokens);
    }

    private static List<String> parseStringArray(JsonNode node) {
        if (node == null || node.isNull()) return List.of();
        try {
            JsonNode actual = node;
            if (node.isTextual()) actual = JSON.readTree(node.asText());
            if (!actual.isArray()) return List.of();
            List<String> values = new ArrayList<>();
            actual.forEach(v -> values.add(v.asText()));
            return values;
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private static void pollBooks() {
        List<Token> tokens = MARKETS.values().stream()
                .flatMap(m -> m.tokens.stream())
                .filter(t -> !t.id.isBlank())
                .limit(500)
                .toList();
        if (tokens.isEmpty()) return;

        // CLOB /books is the batch form; fall back to individual /book calls if needed.
        try {
            ArrayNode params = JSON.createArrayNode();
            for (Token token : tokens) params.add(JSON.createObjectNode().put("token_id", token.id));
            String body = JSON.writeValueAsString(JSON.createObjectNode().set("params", params));
            HttpRequest request = HttpRequest.newBuilder(URI.create(CLOB + "/books"))
                    .timeout(Duration.ofSeconds(25))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                JsonNode books = JSON.readTree(response.body());
                if (books.isArray()) {
                    for (JsonNode b : books) saveBook(b);
                    return;
                }
            }
        } catch (Exception e) {
            log("Batch book request unavailable; using individual books: " + shortError(e));
        }

        List<CompletableFuture<Void>> futures = tokens.stream().limit(80)
                .map(t -> CompletableFuture.runAsync(() -> {
                    try { saveBook(getJson(CLOB + "/book?token_id=" + enc(t.id))); }
                    catch (Exception ignored) {}
                }, HTTP_POOL)).toList();
        futures.forEach(f -> { try { f.get(30, TimeUnit.SECONDS); } catch (Exception ignored) {} });
    }

    private static void saveBook(JsonNode n) {
        String token = text(n, "asset_id", text(n, "market", ""));
        if (token.isBlank()) return;
        double[] bid = bestLevel(n.get("bids"), true);
        double[] ask = bestLevel(n.get("asks"), false);
        Book book = new Book(token, bid[0], bid[1], ask[0], ask[1], System.currentTimeMillis());
        BOOK_CACHE.put(token, book);
        Database.saveBook(book);
    }

    private static double[] bestLevel(JsonNode levels, boolean highest) {
        double bestPrice = 0;
        double bestSize = 0;
        if (levels == null || !levels.isArray()) return new double[]{0, 0};
        for (JsonNode level : levels) {
            double p = number(level, "price", 0);
            double s = number(level, "size", 0);
            if (p <= 0 || s <= 0) continue;
            if (bestPrice == 0 || (highest ? p > bestPrice : p < bestPrice)) {
                bestPrice = p;
                bestSize = s;
            }
        }
        return new double[]{bestPrice, bestSize};
    }

    private static void backfillHistory() {
        long now = System.currentTimeMillis();
        if (now - lastHistoryRefresh.get() < TimeUnit.MINUTES.toMillis(30)) return;
        lastHistoryRefresh.set(now);
        List<Token> tokens = MARKETS.values().stream()
                .flatMap(m -> m.tokens.stream())
                .filter(t -> !t.id.isBlank())
                .limit(MAX_HISTORY_TOKENS_PER_CYCLE)
                .toList();
        if (tokens.isEmpty()) {
            log("Historical backfill waiting for market metadata.");
            return;
        }
        long end = Instant.now().getEpochSecond();
        long start = Instant.now().minus(Duration.ofDays(HISTORY_DAYS)).getEpochSecond();
        AtomicLong points = new AtomicLong();
        List<CompletableFuture<Void>> futures = tokens.stream().map(token ->
                CompletableFuture.runAsync(() -> {
                    try {
                        String url = CLOB + "/prices-history?market=" + enc(token.id)
                                + "&startTs=" + start + "&endTs=" + end + "&fidelity=5";
                        JsonNode response = getJson(url);
                        JsonNode history = response.path("history");
                        if (history.isArray()) {
                            for (JsonNode p : history) {
                                long ts = p.path("t").asLong(0);
                                double price = p.path("p").asDouble(-1);
                                if (ts > 0 && price >= 0 && price <= 1) {
                                    Database.savePrice(token.id, ts, price, "HISTORY");
                                    points.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        log("History fetch failed for token " + abbreviate(token.id) + ": " + shortError(e));
                    }
                }, HTTP_POOL)).toList();
        futures.forEach(f -> { try { f.get(90, TimeUnit.SECONDS); } catch (Exception ignored) {} });
        log("Historical backfill complete: " + points.get() + " price observations processed.");
    }

    // ---------------------------------------------------------------------
    // Research strategies
    // ---------------------------------------------------------------------

    private static void runStrategyScans() {
        if (MARKETS.isEmpty()) return;
        runParityScan();
        runNestedRelationshipScan();
        runReversalScan();
        runResolutionResearch();
        runLeadLagResearch();
    }

    private static void runParityScan() {
        for (Market m : MARKETS.values()) {
            Token yes = findOutcome(m, "yes");
            Token no = findOutcome(m, "no");
            if (yes == null || no == null) continue;
            Book y = BOOK_CACHE.get(yes.id), n = BOOK_CACHE.get(no.id);
            if (y == null || n == null || y.ask <= 0 || n.ask <= 0) continue;
            double cost = y.ask + n.ask;
            double edge = 1.0 - cost;
            if (edge > 0.005) {
                double size = Math.min(y.askSize, n.askSize);
                double maxShares = Math.max(0, size);
                if (maxShares > 0) {
                    recordSignal("YES/NO parity", m, edge,
                            "Buy YES + NO theoretical settlement floor $1; asks=" +
                                    fmt(y.ask) + "+" + fmt(n.ask) + "; matched shares available≈" +
                                    fmt(maxShares) + ". Fees and simultaneous fills still need confirmation.",
                            "OBSERVATION");
                }
            }
        }
    }

    private static void runNestedRelationshipScan() {
        List<Market> list = new ArrayList<>(MARKETS.values());
        // Deliberately conservative: only flag candidate pairs for human review.
        // Titles alone cannot prove logical implication or identical resolution rules.
        for (int i = 0; i < list.size(); i++) {
            Market a = list.get(i);
            String keyA = topicKey(a.question);
            if (keyA.isBlank()) continue;
            for (int j = i + 1; j < Math.min(list.size(), i + 40); j++) {
                Market b = list.get(j);
                if (keyA.equals(topicKey(b.question)) && !a.id.equals(b.id)) {
                    recordSignal("Nested-market relationships", a, 0,
                            "Related-title candidate with market " + b.id +
                                    ". No implication is assumed; inspect both resolution rules before any arbitrage claim.",
                            "REVIEW");
                }
            }
        }
    }

    private static void runReversalScan() {
        for (Market m : MARKETS.values()) {
            for (Token token : m.tokens) {
                try {
                    PricePair pair = Database.priceAround(token.id,
                            Instant.now().minus(Duration.ofMinutes(5)).getEpochSecond());
                    if (pair == null || pair.oldPrice <= 0) continue;
                    Book book = BOOK_CACHE.get(token.id);
                    if (book == null) continue;
                    double move = pair.latestPrice - pair.oldPrice;
                    double threshold = 0.08; // initial 8 percentage-point shock
                    if (Math.abs(move) >= threshold) {
                        Token opposite = oppositeToken(m, token);
                        Book oppositeBook = opposite == null ? null : BOOK_CACHE.get(opposite.id);
                        String direction = move > 0 ? "YES/contract price rose; inspect contrarian NO" :
                                "YES/contract price fell; inspect contrarian YES";
                        double executableAsk = oppositeBook == null ? 0 : oppositeBook.ask;
                        recordSignal("Short-term reversal", m, Math.abs(move),
                                direction + "; five-minute move=" + fmt(move * 100) +
                                        " percentage points; opposite best ask=" + fmt(executableAsk) +
                                        ". No trade is simulated without adequate depth and fees.",
                                "OBSERVATION");
                    }
                } catch (Exception ignored) {}
            }
        }
    }

    private static void runResolutionResearch() {
        long now = System.currentTimeMillis();
        for (Market m : MARKETS.values()) {
            long end = parseDateMillis(m.endDate);
            if (end <= 0) continue;
            long hours = (end - now) / 3_600_000L;
            if (hours >= 0 && hours <= 24) {
                recordSignal("Resolution countdown", m, 0,
                        "Market enters the <=24-hour resolution research bucket (" + hours +
                                " hours by metadata). This is not confirmation of the outcome.",
                        "RESEARCH");
            }
        }
    }

    private static void runLeadLagResearch() {
        // Lightweight candidate generation: markets sharing distinctive title tokens.
        // Actual lag regression is run by the optimizer once enough overlapping points exist.
        List<Market> list = new ArrayList<>(MARKETS.values());
        for (int i = 0; i < list.size(); i++) {
            Market a = list.get(i);
            String key = topicKey(a.question);
            if (key.length() < 8) continue;
            for (int j = i + 1; j < Math.min(list.size(), i + 25); j++) {
                Market b = list.get(j);
                if (key.equals(topicKey(b.question))) {
                    recordSignal("Cross-market lead-lag", a, 0,
                            "Related-market candidate: " + b.question +
                                    ". Pair is queued conceptually; relationship and lag must be measured, not assumed.",
                            "RESEARCH");
                }
            }
        }
    }

    private static void optimize() {
        // A small, transparent parameter sweep over the reversal threshold.
        // Scores use chronological train/validation slices and price-only mark changes.
        List<Double> thresholds = List.of(0.03, 0.05, 0.08, 0.10, 0.12, 0.15);
        int tested = 0;
        for (Market market : MARKETS.values().stream().limit(80).toList()) {
            for (Token token : market.tokens) {
                List<PricePoint> points = Database.recentPrices(token.id, 1200);
                if (points.size() < 80) continue;
                for (double threshold : thresholds) {
                    Score score = evaluateReversal(points, threshold);
                    Database.saveExperiment("Short-term reversal", token.id, threshold,
                            score.train, score.validation, score.trades);
                    tested++;
                }
            }
        }
        if (tested > 0) {
            Database.saveDiscovery("Parameter sweep completed",
                    "Evaluated " + tested + " reversal configurations using chronological training/validation splits. " +
                    "These are price-series research scores, not executable P&L.",
                    "PARAMETER_SEARCH");
            log("Optimizer completed " + tested + " parameter evaluations.");
        }
    }

    private static Score evaluateReversal(List<PricePoint> p, double threshold) {
        int split = (int) (p.size() * 0.70);
        double train = 0, validation = 0;
        int trainN = 0, validationN = 0;
        for (int i = 1; i < p.size() - 3; i++) {
            double shock = p.get(i).price - p.get(i - 1).price;
            if (Math.abs(shock) < threshold) continue;
            double future = p.get(i + 3).price - p.get(i).price;
            double strategyReturn = -Math.signum(shock) * future;
            if (i < split) { train += strategyReturn; trainN++; }
            else { validation += strategyReturn; validationN++; }
        }
        return new Score(trainN == 0 ? 0 : train / trainN,
                validationN == 0 ? 0 : validation / validationN, trainN + validationN);
    }

    private static void recordSignal(String strategy, Market market, double magnitude,
                                     String detail, String kind) {
        // Deduplicate identical strategy/market observations for a short period.
        Database.insertSignal(strategy, market.id, market.question, magnitude, detail, kind);
    }

    // ---------------------------------------------------------------------
    // HTTP server and JSON API
    // ---------------------------------------------------------------------

    private static void startWebServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(8, namedFactory("web")));
        server.createContext("/", Main::serveIndex);
        server.createContext("/api/state", ex -> json(ex, Database.state()));
        server.createContext("/api/strategies", ex -> json(ex, Database.strategyStats()));
        server.createContext("/api/discoveries", ex -> json(ex, Database.discoveries()));
        server.createContext("/api/signals", ex -> json(ex, Database.signals()));
        server.createContext("/api/experiments", ex -> json(ex, Database.experiments()));
        server.createContext("/api/logs", ex -> json(ex, Map.of("logs", List.copyOf(RECENT_LOGS))));
        server.createContext("/health", ex -> json(ex, Map.of("status", systemStatus, "time", Instant.now().toString())));
        server.start();
    }

    private static void serveIndex(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.sendResponseHeaders(405, -1); return;
        }
        String path = ex.getRequestURI().getPath();
        if (!path.equals("/") && !path.equals("/index.html")) {
            send(ex, 404, "Not found", "text/plain; charset=utf-8"); return;
        }
        Path file = Path.of("/app/index.html");
        if (!Files.exists(file)) file = Path.of("index.html");
        if (!Files.exists(file)) {
            send(ex, 500, "index.html not found in container", "text/plain; charset=utf-8"); return;
        }
        send(ex, 200, Files.readString(file), "text/html; charset=utf-8");
    }

    private static void json(HttpExchange ex, Object data) throws IOException {
        try {
            byte[] bytes = JSON.writeValueAsBytes(data);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.getResponseHeaders().set("Cache-Control", "no-store");
            ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
        } finally { ex.close(); }
    }

    private static void send(HttpExchange ex, int code, String body, String type) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.sendResponseHeaders(code, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    // ---------------------------------------------------------------------
    // Utilities / domain objects
    // ---------------------------------------------------------------------

    private static JsonNode getJson(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(25))
                .header("Accept", "application/json")
                .header("User-Agent", "PolymarketResearchLab/1.0")
                .GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300)
            throw new IOException("HTTP " + response.statusCode() + " from " + url);
        return JSON.readTree(response.body());
    }

    private static Token findOutcome(Market m, String name) {
        return m.tokens.stream().filter(t -> t.outcome.equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    private static Token oppositeToken(Market m, Token token) {
        return m.tokens.stream().filter(t -> !t.id.equals(token.id)).findFirst().orElse(null);
    }

    private static String topicKey(String title) {
        String s = title.toLowerCase(Locale.ROOT)
                .replaceAll("\\b(will|the|a|an|be|by|before|after|above|below|over|under|reach|hit|to|in|on|at|of|and|or)\\b", " ")
                .replaceAll("[^a-z0-9 ]", " ")
                .replaceAll("\\s+", " ").trim();
        return s.length() > 90 ? s.substring(0, 90) : s;
    }

    private static long parseDateMillis(String value) {
        if (value == null || value.isBlank()) return -1;
        try { return OffsetDateTime.parse(value).toInstant().toEpochMilli(); }
        catch (DateTimeParseException ignored) {
            try { return Instant.parse(value).toEpochMilli(); }
            catch (Exception e) { return -1; }
        }
    }

    private static String text(JsonNode n, String key, String fallback) {
        JsonNode v = n == null ? null : n.get(key);
        return v == null || v.isNull() ? fallback : v.asText(fallback);
    }

    private static double number(JsonNode n, String key, double fallback) {
        JsonNode v = n == null ? null : n.get(key);
        if (v == null || v.isNull()) return fallback;
        try { return v.isNumber() ? v.asDouble() : Double.parseDouble(v.asText()); }
        catch (Exception e) { return fallback; }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String fmt(double d) { return String.format(Locale.US, "%.4f", d); }
    private static String abbreviate(String s) { return s.length() <= 12 ? s : s.substring(0, 8) + "…"; }
    private static String shortError(Throwable e) {
        String m = e.getMessage();
        return e.getClass().getSimpleName() + (m == null ? "" : ": " + m.substring(0, Math.min(180, m.length())));
    }
    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }
    private static ThreadFactory namedFactory(String prefix) {
        AtomicLong count = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + count.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
    private static void safeRun(String name, Runnable task) {
        try { task.run(); }
        catch (Throwable t) { log(name + " failed: " + shortError(t)); }
    }
    private static void log(String message) {
        String line = Instant.now() + " " + message;
        System.out.println(line);
        RECENT_LOGS.addFirst(line);
        while (RECENT_LOGS.size() > 150) RECENT_LOGS.removeLast();
        try { Database.log(line); } catch (Exception ignored) {}
    }
    private static void shutdown() {
        if (!STOPPING.compareAndSet(false, true)) return;
        systemStatus = "STOPPING";
        log("Shutdown requested; stopping scheduled work.");
        SCHEDULER.shutdown();
        HTTP_POOL.shutdown();
        BACKTEST_POOL.shutdown();
        try {
            SCHEDULER.awaitTermination(5, TimeUnit.SECONDS);
            HTTP_POOL.awaitTermination(5, TimeUnit.SECONDS);
            BACKTEST_POOL.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        systemStatus = "STOPPED";
    }

    private record Token(String outcome, String id) {}
    private record Market(String id, String question, String slug, String endDate,
                          double volume, List<Token> tokens) {}
    private record Book(String token, double bid, double bidSize, double ask, double askSize, long ts) {}
    private record PricePair(double oldPrice, double latestPrice) {}
    private record PricePoint(long ts, double price) {}
    private record Score(double train, double validation, int trades) {}

    // ---------------------------------------------------------------------
    // SQLite
    // ---------------------------------------------------------------------

    private static final class Database {
        private static Connection open() throws SQLException {
            Connection c = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);
            try (Statement s = c.createStatement()) {
                s.execute("PRAGMA journal_mode=WAL");
                s.execute("PRAGMA busy_timeout=5000");
                s.execute("PRAGMA foreign_keys=ON");
            }
            return c;
        }

        static void init() throws SQLException {
            try (Connection c = open(); Statement s = c.createStatement()) {
                s.execute("""
                    CREATE TABLE IF NOT EXISTS markets(
                      id TEXT PRIMARY KEY, question TEXT, slug TEXT, end_date TEXT,
                      volume REAL, token_json TEXT, updated_at INTEGER)
                    """);
                s.execute("""
                    CREATE TABLE IF NOT EXISTS prices(
                      token_id TEXT NOT NULL, ts INTEGER NOT NULL, price REAL NOT NULL,
                      source TEXT NOT NULL, PRIMARY KEY(token_id,ts,source))
                    """);
                s.execute("CREATE INDEX IF NOT EXISTS idx_prices_token_ts ON prices(token_id,ts)");
                s.execute("""
                    CREATE TABLE IF NOT EXISTS books(
                      token_id TEXT NOT NULL, ts INTEGER NOT NULL, bid REAL, bid_size REAL,
                      ask REAL, ask_size REAL, PRIMARY KEY(token_id,ts))
                    """);
                s.execute("""
                    CREATE TABLE IF NOT EXISTS signals(
                      id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT, market_id TEXT,
                      question TEXT, magnitude REAL, detail TEXT, kind TEXT, created_at INTEGER)
                    """);
                s.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at)");
                s.execute("""
                    CREATE TABLE IF NOT EXISTS experiments(
                      id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT, token_id TEXT,
                      parameter REAL, train_score REAL, validation_score REAL,
                      trades INTEGER, created_at INTEGER)
                    """);
                s.execute("""
                    CREATE TABLE IF NOT EXISTS discoveries(
                      id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, body TEXT,
                      category TEXT, created_at INTEGER)
                    """);
                s.execute("""
                    CREATE TABLE IF NOT EXISTS system_logs(
                      id INTEGER PRIMARY KEY AUTOINCREMENT, line TEXT, created_at INTEGER)
                    """);
            }
        }

        static void saveMarket(Market m) {
            String tokenJson;
            try { tokenJson = JSON.writeValueAsString(m.tokens); }
            catch (Exception e) { tokenJson = "[]"; }
            try (Connection c = open(); PreparedStatement p = c.prepareStatement("""
                INSERT INTO markets(id,question,slug,end_date,volume,token_json,updated_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET question=excluded.question,slug=excluded.slug,
                end_date=excluded.end_date,volume=excluded.volume,token_json=excluded.token_json,
                updated_at=excluded.updated_at
                """)) {
                p.setString(1,m.id); p.setString(2,m.question); p.setString(3,m.slug);
                p.setString(4,m.endDate); p.setDouble(5,m.volume); p.setString(6,tokenJson);
                p.setLong(7,System.currentTimeMillis()); p.executeUpdate();
            } catch (SQLException e) { log("DB saveMarket: " + shortError(e)); }
        }

        static void savePrice(String token, long ts, double price, String source) {
            try (Connection c = open(); PreparedStatement p = c.prepareStatement(
                    "INSERT OR REPLACE INTO prices(token_id,ts,price,source) VALUES(?,?,?,?)")) {
                p.setString(1,token); p.setLong(2,ts); p.setDouble(3,price); p.setString(4,source);
                p.executeUpdate();
            } catch (SQLException e) { log("DB savePrice: " + shortError(e)); }
        }

        static void saveBook(Book b) {
            try (Connection c = open(); PreparedStatement p = c.prepareStatement(
                    "INSERT OR REPLACE INTO books(token_id,ts,bid,bid_size,ask,ask_size) VALUES(?,?,?,?,?,?)")) {
                p.setString(1,b.token); p.setLong(2,b.ts); p.setDouble(3,b.bid); p.setDouble(4,b.bidSize);
                p.setDouble(5,b.ask); p.setDouble(6,b.askSize); p.executeUpdate();
            } catch (SQLException e) { log("DB saveBook: " + shortError(e)); }
        }

        static PricePair priceAround(String token, long targetTs) {
            try (Connection c = open(); PreparedStatement p = c.prepareStatement("""
                SELECT
                  (SELECT price FROM prices WHERE token_id=? AND ts<=? ORDER BY ts DESC LIMIT 1),
                  (SELECT price FROM prices WHERE token_id=? ORDER BY ts DESC LIMIT 1)
                """)) {
                p.setString(1,token); p.setLong(2,targetTs); p.setString(3,token);
                try (ResultSet r=p.executeQuery()) {
                    if (r.next()) {
                        double old=r.getDouble(1), latest=r.getDouble(2);
                        if (!r.wasNull() && old > 0 && latest > 0) return new PricePair(old,latest);
                    }
                }
            } catch (SQLException e) { log("DB priceAround: " + shortError(e)); }
            return null;
        }

        static List<PricePoint> recentPrices(String token, int limit) {
            List<PricePoint> out = new ArrayList<>();
            try (Connection c=open(); PreparedStatement p=c.prepareStatement(
                    "SELECT ts,price FROM prices WHERE token_id=? ORDER BY ts DESC LIMIT ?")) {
                p.setString(1,token); p.setInt(2,limit);
                try (ResultSet r=p.executeQuery()) {
                    while(r.next()) out.add(new PricePoint(r.getLong(1),r.getDouble(2)));
                }
            } catch(SQLException e) { log("DB recentPrices: " + shortError(e)); }
            Collections.reverse(out);
            return out;
        }

        static void insertSignal(String strategy, String marketId, String question,
                                 double magnitude, String detail, String kind) {
            try (Connection c=open(); PreparedStatement p=c.prepareStatement("""
                INSERT INTO signals(strategy,market_id,question,magnitude,detail,kind,created_at)
                SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS(
                  SELECT 1 FROM signals WHERE strategy=? AND market_id=? AND detail=? AND created_at>?)
                """)) {
                long now=System.currentTimeMillis();
                p.setString(1,strategy); p.setString(2,marketId); p.setString(3,question);
                p.setDouble(4,magnitude); p.setString(5,detail); p.setString(6,kind); p.setLong(7,now);
                p.setString(8,strategy); p.setString(9,marketId); p.setString(10,detail);
                p.setLong(11,now-TimeUnit.MINUTES.toMillis(30)); p.executeUpdate();
            } catch(SQLException e) { log("DB insertSignal: " + shortError(e)); }
        }

        static void saveExperiment(String strategy,String token,double parameter,double train,
                                   double validation,int trades) {
            try(Connection c=open();PreparedStatement p=c.prepareStatement("""
                INSERT INTO experiments(strategy,token_id,parameter,train_score,validation_score,trades,created_at)
                VALUES(?,?,?,?,?,?,?)
                """)) {
                p.setString(1,strategy);p.setString(2,token);p.setDouble(3,parameter);
                p.setDouble(4,train);p.setDouble(5,validation);p.setInt(6,trades);
                p.setLong(7,System.currentTimeMillis());p.executeUpdate();
            } catch(SQLException e) { log("DB saveExperiment: " + shortError(e)); }
        }

        static void saveDiscovery(String title,String body,String category) {
            try(Connection c=open();PreparedStatement p=c.prepareStatement(
                    "INSERT INTO discoveries(title,body,category,created_at) VALUES(?,?,?,?)")) {
                p.setString(1,title);p.setString(2,body);p.setString(3,category);
                p.setLong(4,System.currentTimeMillis());p.executeUpdate();
            } catch(SQLException e) { log("DB discovery: " + shortError(e)); }
        }

        static void log(String line) {
            try(Connection c=open();PreparedStatement p=c.prepareStatement(
                    "INSERT INTO system_logs(line,created_at) VALUES(?,?)")) {
                p.setString(1,line);p.setLong(2,System.currentTimeMillis());p.executeUpdate();
            } catch(SQLException ignored) {}
        }

        static Map<String,Object> state() {
            Map<String,Object> m=new LinkedHashMap<>();
            m.put("status",systemStatus);m.put("uptimeSeconds",(System.currentTimeMillis()-startedAt)/1000);
            m.put("markets",MARKETS.size());m.put("cachedBooks",BOOK_CACHE.size());
            m.put("database",DB_FILE.toString());m.put("historyDays",HISTORY_DAYS);
            m.put("strategies",STRATEGY_NAMES.size());m.put("mode","PAPER / RESEARCH ONLY");
            m.put("lastMarketRefresh",lastMarketRefresh.get());m.put("lastHistoryRefresh",lastHistoryRefresh.get());
            m.put("signals",count("signals"));m.put("pricePoints",count("prices"));m.put("bookSnapshots",count("books"));
            return m;
        }

        static long count(String table) {
            if (!Set.of("signals","prices","books","markets","experiments","discoveries").contains(table)) return 0;
            try(Connection c=open();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+table)) {
                return r.next()?r.getLong(1):0;
            } catch(SQLException e) { return 0; }
        }

        static List<Map<String,Object>> strategyStats() {
            List<Map<String,Object>> out=new ArrayList<>();
            for(String name:STRATEGY_NAMES) {
                Map<String,Object> row=new LinkedHashMap<>();row.put("name",name);
                try(Connection c=open();PreparedStatement p=c.prepareStatement(
                        "SELECT COUNT(*),MAX(created_at) FROM signals WHERE strategy=?")) {
                    p.setString(1,name);try(ResultSet r=p.executeQuery()){
                        if(r.next()){row.put("signals",r.getLong(1));row.put("lastSignal",r.getLong(2));}
                    }
                }catch(SQLException ignored){}
                out.add(row);
            }
            return out;
        }

        static List<Map<String,Object>> discoveries() {
            return queryRows("SELECT id,title,body,category,created_at FROM discoveries ORDER BY id DESC LIMIT 30");
        }
        static List<Map<String,Object>> signals() {
            return queryRows("SELECT id,strategy,question,magnitude,detail,kind,created_at FROM signals ORDER BY id DESC LIMIT 60");
        }
        static List<Map<String,Object>> experiments() {
            return queryRows("SELECT id,strategy,parameter,train_score,validation_score,trades,created_at FROM experiments ORDER BY id DESC LIMIT 100");
        }
        static List<Map<String,Object>> queryRows(String sql) {
            List<Map<String,Object>> out=new ArrayList<>();
            try(Connection c=open();Statement s=c.createStatement();ResultSet r=s.executeQuery(sql)){
                ResultSetMetaData md=r.getMetaData();
                while(r.next()){
                    Map<String,Object> row=new LinkedHashMap<>();
                    for(int i=1;i<=md.getColumnCount();i++)row.put(md.getColumnLabel(i),r.getObject(i));
                    out.add(row);
                }
            }catch(SQLException e){log("DB queryRows: "+shortError(e));}
            return out;
        }
    }
}
