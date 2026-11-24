
package lab;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.sun.net.httpserver.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public final class Main {
    static final ObjectMapper J = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL).build();

    static final String DB = System.getenv().getOrDefault("DB_FILE", "/app/data/lab.db");
    static final int PORT = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
    static final int MAX = Integer.parseInt(System.getenv().getOrDefault("MAX_MARKETS", "2500"));
    static final int INTERVAL = Integer.parseInt(System.getenv().getOrDefault("SCAN_SECONDS", "90"));

    static final ExecutorService POOL = Executors.newFixedThreadPool(4);
    static final ScheduledExecutorService TIMER = Executors.newScheduledThreadPool(2);
    static final Map<String, Market> MARKETS = new ConcurrentHashMap<>();

    static volatile String status = "STARTING";
    static volatile String lastPair = "None evaluated";
    static volatile long lastScan = 0;
    static volatile long evaluated = 0;
    static volatile long started = System.currentTimeMillis();

    record Market(
            String venue, String id, String question, String url,
            String end, double price, String currency,
            String entity, String predicate, double threshold,
            String operator, String fingerprint
    ) {}

    record Pair(
            Market a, Market b, String relation,
            double confidence, double discrepancy, String explanation
    ) {}

    public static void main(String[] args) throws Exception {
        Files.createDirectories(Path.of(DB).getParent());
        initDB();
        startWeb();

        status = "ONLINE";

        TIMER.scheduleWithFixedDelay(() -> {
            try { scan(); }
            catch (Throwable e) {
                status = "ERROR: " + e.getMessage();
                log("Scan error: " + e);
            }
        }, 0, INTERVAL, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            status = "STOPPING";
            TIMER.shutdown();
            POOL.shutdown();
        }));

        log("Research engine started on port " + PORT);
    }

    // ============================================================
    // MARKET INGESTION
    // ============================================================

    static void scan() {
        status = "INGESTING";

        List<CompletableFuture<Void>> jobs = List.of(
                CompletableFuture.runAsync(Main::polymarket, POOL),
                CompletableFuture.runAsync(Main::kalshi, POOL),
                CompletableFuture.runAsync(Main::manifold, POOL)
        );

        for (var job : jobs) {
            try { job.get(45, TimeUnit.SECONDS); }
            catch (Exception e) { log("Provider timeout: " + e.getMessage()); }
        }

        status = "MATCHING";

        List<Market> markets = new ArrayList<>(MARKETS.values());
        if (markets.size() > MAX) {
            markets.sort(Comparator.comparingDouble(
                    (Market m) -> m.price > 0 ? 1 : 0).reversed());
            markets = new ArrayList<>(markets.subList(0, MAX));
        }

        for (Market m : markets) saveMarket(m);

        List<Pair> candidates = generatePairs(markets);

        status = "EVALUATING";
        evaluated = 0;

        for (Pair p : candidates) {
            savePair(p);
            lastPair = p.a.venue + " / " + p.b.venue + " | "
                    + p.relation + " | " + p.a.question + " <> " + p.b.question;
            evaluated++;
        }

        lastScan = System.currentTimeMillis();
        status = "ONLINE";

        log("Scan finished. Markets=" + markets.size()
                + ", candidate pairs=" + candidates.size());
    }

    static void polymarket() {
        try {
            JsonNode data = get(
                    "https://gamma-api.polymarket.com/markets"
                    + "?active=true&closed=false&limit=500");

            if (!data.isArray()) return;

            for (JsonNode n : data) {
                String id = s(n, "conditionId", s(n, "id", ""));
                String q = s(n, "question", "");
                if (id.isBlank() || q.isBlank()) continue;

                double price = 0;
                try {
                    JsonNode prices = n.get("outcomePrices");
                    if (prices != null) {
                        if (prices.isTextual()) prices = J.readTree(prices.asText());
                        if (prices.isArray() && prices.size() > 0)
                            price = Double.parseDouble(prices.get(0).asText());
                    }
                } catch (Exception ignored) {}

                add(new Market("POLYMARKET", id, q,
                        "https://polymarket.com/event/" + s(n, "slug", ""),
                        s(n, "endDate", ""), price, "USD",
                        "", "", 0, "", ""));
            }
        } catch (Exception e) { log("Polymarket: " + e.getMessage()); }
    }

    static void kalshi() {
        try {
            JsonNode data = get(
                    "https://api.elections.kalshi.com/trade-api/v2/markets"
                    + "?status=open&limit=200");

            JsonNode markets = data.path("markets");
            if (!markets.isArray()) return;

            for (JsonNode n : markets) {
                String id = s(n, "ticker", "");
                String q = s(n, "title", "");
                if (id.isBlank() || q.isBlank()) continue;

                double price = number(n, "yes_ask_dollars",
                        number(n, "last_price_dollars", 0));

                if (price > 1) price /= 100.0;

                add(new Market("KALSHI", id, q,
                        "https://kalshi.com/markets/" + id,
                        s(n, "close_time", ""), price, "USD",
                        "", "", 0, "", ""));
            }
        } catch (Exception e) { log("Kalshi: " + e.getMessage()); }
    }

    static void manifold() {
        try {
            JsonNode data = get(
                    "https://api.manifold.markets/v0/markets?limit=500");

            if (!data.isArray()) return;

            for (JsonNode n : data) {
                String id = s(n, "id", "");
                String q = s(n, "question", "");
                if (id.isBlank() || q.isBlank()) continue;

                double price = number(n, "probability", 0);

                add(new Market("MANIFOLD", id, q,
                        s(n, "url", ""),
                        s(n, "closeTime", ""), price, "MANA",
                        "", "", 0, "", ""));
            }
        } catch (Exception e) { log("Manifold: " + e.getMessage()); }
    }

    static void add(Market raw) {
        Market m = normalize(raw);
        MARKETS.put(m.venue + ":" + m.id, m);
    }

    // ============================================================
    // CONTRACT NORMALIZATION
    // ============================================================

    static Market normalize(Market m) {
        String q = m.question.toLowerCase(Locale.ROOT);

        String clean = q.replaceAll("[^a-z0-9 ]", " ")
                .replaceAll("\\s+", " ").trim();

        String entity = clean;
        double threshold = 0;
        String operator = "";
        String predicate = "";

        Pattern p = Pattern.compile(
                "(above|over|exceed|greater than|below|under|less than|reach|hit|at least|at most)\\s*\\$?([0-9,]+(?:\\.[0-9]+)?)",
                Pattern.CASE_INSENSITIVE);

        Matcher matcher = p.matcher(clean);

        if (matcher.find()) {
            predicate = matcher.group(1);
            try {
                threshold = Double.parseDouble(
                        matcher.group(2).replace(",", ""));
            } catch (Exception ignored) {}

            operator = switch (predicate) {
                case "above", "over", "exceed", "greater than", "reach", "hit" -> "GT";
                case "below", "under", "less than" -> "LT";
                case "at least" -> "GTE";
                case "at most" -> "LTE";
                default -> "";
            };

            entity = clean.substring(0, matcher.start()).trim();
        }

        entity = entity.replaceAll(
                "\\b(will|the|a|an|be|by|before|after|in|on|at|of|and|or|to|this|next)\\b",
                " ").replaceAll("\\s+", " ").trim();

        String fingerprint = fingerprint(clean);

        return new Market(m.venue, m.id, m.question, m.url,
                m.end, m.price, m.currency, entity,
                predicate, threshold, operator, fingerprint);
    }

    static String fingerprint(String text) {
        Set<String> stop = Set.of(
                "will", "the", "a", "an", "be", "by", "before",
                "after", "in", "on", "at", "of", "and", "or",
                "to", "is", "it", "this", "that", "what");

        return Arrays.stream(text.toLowerCase().split("\\W+"))
                .filter(x -> x.length() > 2 && !stop.contains(x))
                .distinct().sorted()
                .collect(java.util.stream.Collectors.joining(" "));
    }

    // ============================================================
    // CANDIDATE GENERATION
    // ============================================================

    static List<Pair> generatePairs(List<Market> markets) {
        Map<String, List<Market>> index = new HashMap<>();

        for (Market m : markets) {
            for (String token : m.fingerprint.split(" ")) {
                if (token.length() < 3) continue;
                index.computeIfAbsent(token, k -> new ArrayList<>()).add(m);
            }
        }

        Set<String> seen = new HashSet<>();
        List<Pair> results = new ArrayList<>();

        for (List<Market> group : index.values()) {
            if (group.size() > 80) continue;

            for (int i = 0; i < group.size(); i++) {
                for (int j = i + 1; j < group.size(); j++) {
                    Market a = group.get(i);
                    Market b = group.get(j);

                    if (a.venue.equals(b.venue) && a.id.equals(b.id)) continue;

                    String key = a.venue.compareTo(b.venue) < 0
                            ? a.venue + ":" + a.id + "|" + b.venue + ":" + b.id
                            : b.venue + ":" + b.id + "|" + a.venue + ":" + a.id;

                    if (!seen.add(key)) continue;

                    Pair pair = classify(a, b);
                    if (pair != null) results.add(pair);
                }
            }
        }

        results.sort(Comparator.comparingDouble(
                (Pair p) -> p.confidence + p.discrepancy).reversed());

        return results;
    }

    // ============================================================
    // RELATIONSHIP CLASSIFICATION
    // ============================================================

    static Pair classify(Market a, Market b) {
        String relation;
        String explanation;
        double confidence = 0;

        double similarity = similarity(a.fingerprint, b.fingerprint);

        boolean sameEntity = !a.entity.isBlank()
                && !b.entity.isBlank()
                && similarity(a.entity, b.entity) >= 0.65;

        boolean sameThreshold = sameEntity
                && a.operator.equals(b.operator)
                && a.threshold == b.threshold
                && !a.operator.isBlank();

        boolean nested = sameEntity
                && !a.operator.isBlank()
                && a.operator.equals(b.operator)
                && a.threshold > 0 && b.threshold > 0
                && a.threshold != b.threshold;

        boolean complement = similarity >= 0.80
                && ((a.question.toLowerCase().contains("not ")
                && !b.question.toLowerCase().contains("not "))
                || (b.question.toLowerCase().contains("not ")
                && !a.question.toLowerCase().contains("not ")));

        if (sameThreshold || similarity >= 0.94) {
            relation = "EQUIVALENT";
            confidence = Math.max(similarity, 0.95);
            explanation = "Highly similar contract wording and/or matching normalized proposition. Verify resolution rules.";
        } else if (nested) {
            relation = "NESTED";
            confidence = 0.90;
            explanation = "Same underlying entity and direction, different thresholds. Potential logical implication.";
        } else if (complement) {
            relation = "COMPLEMENT";
            confidence = similarity;
            explanation = "Possible opposite propositions. Verify that both contracts describe exact complements.";
        } else if (sameEntity && similarity >= 0.40) {
            relation = "DEPENDENT";
            confidence = similarity * 0.85;
            explanation = "Shared underlying entity and overlapping contract vocabulary. Dependency is not proven.";
        } else if (similarity >= 0.48) {
            relation = "RELATED";
            confidence = similarity;
            explanation = "Lexical relationship candidate. Requires human review.";
        } else {
            return null;
        }

        double discrepancy = 0;
        if (a.price > 0 && b.price > 0) {
            discrepancy = Math.abs(a.price - b.price);
        }

        return new Pair(a, b, relation, confidence,
                discrepancy, explanation);
    }

    static double similarity(String a, String b) {
        Set<String> x = new HashSet<>(Arrays.asList(a.split(" ")));
        Set<String> y = new HashSet<>(Arrays.asList(b.split(" ")));

        x.remove("");
        y.remove("");

        if (x.isEmpty() || y.isEmpty()) return 0;

        Set<String> intersection = new HashSet<>(x);
        intersection.retainAll(y);

        Set<String> union = new HashSet<>(x);
        union.addAll(y);

        return (double) intersection.size() / union.size();
    }

    // ============================================================
    // SQLITE: PERSISTENT DATA
    // ============================================================

    static Connection db() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:sqlite:" + DB);
        try (Statement s = c.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("PRAGMA busy_timeout=10000");
            s.execute("PRAGMA synchronous=NORMAL");
        }
        return c;
    }

    static void initDB() throws SQLException {
        try (Connection c = db(); Statement s = c.createStatement()) {
            s.execute("""
                CREATE TABLE IF NOT EXISTS markets(
                    venue TEXT, id TEXT, question TEXT, url TEXT,
                    end_date TEXT, price REAL, fingerprint TEXT,
                    updated INTEGER,
                    PRIMARY KEY(venue,id))
                """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS pairs(
                    pair_key TEXT PRIMARY KEY,
                    venue_a TEXT, id_a TEXT, question_a TEXT, url_a TEXT,
                    venue_b TEXT, id_b TEXT, question_b TEXT, url_b TEXT,
                    relation TEXT, confidence REAL, discrepancy REAL,
                    explanation TEXT, first_seen INTEGER, last_seen INTEGER,
                    evaluations INTEGER DEFAULT 1)
                """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS evaluations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_key TEXT, relation TEXT, confidence REAL,
                    discrepancy REAL, created INTEGER)
                """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT, created INTEGER)
                """);

            s.execute("CREATE INDEX IF NOT EXISTS idx_pairs_conf ON pairs(confidence)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_eval_pair ON evaluations(pair_key)");
        }
    }

    static void saveMarket(Market m) {
        try (Connection c = db(); PreparedStatement p = c.prepareStatement("""
            INSERT INTO markets VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(venue,id) DO UPDATE SET
            question=excluded.question,url=excluded.url,
            end_date=excluded.end_date,price=excluded.price,
            fingerprint=excluded.fingerprint,updated=excluded.updated
            """)) {
            p.setString(1, m.venue);
            p.setString(2, m.id);
            p.setString(3, m.question);
            p.setString(4, m.url);
            p.setString(5, m.end);
            p.setDouble(6, m.price);
            p.setString(7, m.fingerprint);
            p.setLong(8, System.currentTimeMillis());
            p.executeUpdate();
        } catch (Exception e) { log("Market DB: " + e.getMessage()); }
    }

    static void savePair(Pair p) {
        String key = pairKey(p.a, p.b);
        long now = System.currentTimeMillis();

        try (Connection c = db()) {
            try (PreparedStatement q = c.prepareStatement("""
                INSERT INTO pairs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                ON CONFLICT(pair_key) DO UPDATE SET
                relation=excluded.relation,
                confidence=excluded.confidence,
                discrepancy=excluded.discrepancy,
                explanation=excluded.explanation,
                last_seen=excluded.last_seen,
                evaluations=pairs.evaluations+1
                """)) {
                q.setString(1, key);
                q.setString(2, p.a.venue);
                q.setString(3, p.a.id);
                q.setString(4, p.a.question);
                q.setString(5, p.a.url);
                q.setString(6, p.b.venue);
                q.setString(7, p.b.id);
                q.setString(8, p.b.question);
                q.setString(9, p.b.url);
                q.setString(10, p.relation);
                q.setDouble(11, p.confidence);
                q.setDouble(12, p.discrepancy);
                q.setString(13, p.explanation);
                q.setLong(14, now);
                q.setLong(15, now);
                q.executeUpdate();
            }

            try (PreparedStatement q = c.prepareStatement("""
                INSERT INTO evaluations(pair_key,relation,confidence,discrepancy,created)
                VALUES(?,?,?,?,?)
                """)) {
                q.setString(1, key);
                q.setString(2, p.relation);
                q.setDouble(3, p.confidence);
                q.setDouble(4, p.discrepancy);
                q.setLong(5, now);
                q.executeUpdate();
            }
        } catch (Exception e) { log("Pair DB: " + e.getMessage()); }
    }

    static String pairKey(Market a, Market b) {
        String x = a.venue + ":" + a.id;
        String y = b.venue + ":" + b.id;
        return x.compareTo(y) < 0 ? x + "|" + y : y + "|" + x;
    }

    // ============================================================
    // WEB SERVER
    // ============================================================

    static void startWeb() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(POOL);

        server.createContext("/", ex -> {
            send(ex, html(), "text/html");
        });

        server.createContext("/api/state", ex -> {
            Map<String,Object> m = new LinkedHashMap<>();
            m.put("status", status);
            m.put("uptimeSeconds", (System.currentTimeMillis() - started) / 1000);
            m.put("markets", MARKETS.size());
            m.put("pairs", count("pairs"));
            m.put("evaluations", count("evaluations"));
            m.put("lastScan", lastScan);
            m.put("lastPair", lastPair);
            m.put("evaluatedThisScan", evaluated);
            m.put("database", DB);
            sendJson(ex, m);
        });

        server.createContext("/api/pairs", ex -> {
            sendJson(ex, rows("""
                SELECT * FROM pairs
                ORDER BY confidence DESC, discrepancy DESC
                LIMIT 500
                """));
        });

        server.createContext("/api/events", ex -> {
            sendJson(ex, rows(
                    "SELECT * FROM events ORDER BY id DESC LIMIT 50"));
        });

        server.createContext("/health", ex -> {
            sendJson(ex, Map.of("status", status));
        });

        server.start();
    }

    static long count(String table) {
        try (Connection c = db(); Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return r.next() ? r.getLong(1) : 0;
        } catch (Exception e) { return 0; }
    }

    static List<Map<String,Object>> rows(String sql) {
        List<Map<String,Object>> out = new ArrayList<>();
        try (Connection c = db(); Statement s = c.createStatement();
             ResultSet r = s.executeQuery(sql)) {
            ResultSetMetaData md = r.getMetaData();
            while (r.next()) {
                Map<String,Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++)
                    row.put(md.getColumnLabel(i), r.getObject(i));
                out.add(row);
            }
        } catch (Exception e) { log("Query: " + e.getMessage()); }
        return out;
    }

    static void sendJson(HttpExchange ex, Object o) throws java.io.IOException {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        byte[] b = J.writeValueAsBytes(o);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(200, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    static void send(HttpExchange ex, String body, String type)
            throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
        ex.sendResponseHeaders(200, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    // ============================================================
    // DASHBOARD
    // ============================================================

    static String html() {
        return """
        <!DOCTYPE html>
        <html>
        <head>
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Market Relationship Lab</title>
        <style>
        *{box-sizing:border-box}
        body{background:#0b1020;color:#e5e7eb;font-family:Arial;margin:0;padding:25px}
        h1{font-size:25px}
        .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px}
        .card{background:#151e32;border:1px solid #293650;border-radius:10px;padding:18px}
        .number{font-size:28px;font-weight:bold;margin-top:9px}
        .muted{color:#94a3b8;font-size:12px}
        .green{color:#34d399}
        .orange{color:#fbbf24}
        .tablewrap{overflow-x:auto;margin-top:20px}
        table{width:100%;border-collapse:collapse;min-width:1000px}
        td,th{padding:12px;border-bottom:1px solid #293650;text-align:left;font-size:13px}
        th{color:#94a3b8}
        a{color:#60a5fa;text-decoration:none}
        button{background:#2563eb;color:white;border:0;padding:10px 16px;border-radius:6px;cursor:pointer}
        .bar{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}
        </style>
        </head>
        <body>
        <div class="bar">
        <div><h1>Market Relationship Lab</h1>
        <div class="muted">Cross-platform prediction market research</div></div>
        <button onclick="refresh()">Refresh</button>
        </div>
        <br>
        <div class="grid">
          <div class="card">SERVER STATUS<div class="number green" id="status">...</div></div>
          <div class="card">MARKETS<div class="number" id="markets">0</div></div>
          <div class="card">SAVED PAIRS<div class="number" id="pairs">0</div></div>
          <div class="card">EVALUATIONS<div class="number" id="evaluations">0</div></div>
          <div class="card">UPTIME<div class="number" id="uptime">0</div></div>
          <div class="card">LAST SCAN<div class="number" id="lastscan">Never</div></div>
        </div>
        <br>
        <div class="card">
        <h3>Last evaluated pair</h3>
        <div id="lastpair" class="muted">Waiting...</div>
        <p class="muted" id="database"></p>
        </div>
        <div class="bar"><h2>Discovered relationships</h2>
        <span class="muted">Sorted by confidence and discrepancy</span></div>
        <div class="tablewrap">
        <table>
        <thead><tr>
        <th>Relationship</th><th>Confidence</th><th>Difference</th>
        <th>Market A</th><th>Market B</th><th>Explanation</th><th>Evaluations</th>
        </tr></thead>
        <tbody id="table"></tbody>
        </table>
        </div>
        <script>
        const fmt=x=>Number(x||0).toFixed(3);
        async function refresh(){
          try{
            let s=await (await fetch('/api/state')).json();
            for(let k of ['status','markets','pairs','evaluations'])
              document.getElementById(k).textContent=s[k];
            document.getElementById('uptime').textContent=
              Math.floor(s.uptimeSeconds/3600)+'h '+Math.floor(s.uptimeSeconds%3600/60)+'m';
            document.getElementById('lastscan').textContent=
              s.lastScan?new Date(s.lastScan).toLocaleTimeString():'Never';
            document.getElementById('lastpair').textContent=s.lastPair;
            document.getElementById('database').textContent='Database: '+s.database;
            let rows=await (await fetch('/api/pairs')).json();
            document.getElementById('table').innerHTML=rows.map(p=>`
              <tr>
              <td><b>${p.relation}</b></td>
              <td>${fmt(p.confidence)}</td>
              <td>${fmt(p.discrepancy)}</td>
              <td><a target="_blank" href="${p.url_a||'#'}">${p.venue_a}: ${p.question_a}</a></td>
              <td><a target="_blank" href="${p.url_b||'#'}">${p.venue_b}: ${p.question_b}</a></td>
              <td>${p.explanation}</td>
              <td>${p.evaluations}</td>
              </tr>`).join('');
          }catch(e){console.log(e)}
        }
        refresh();setInterval(refresh,10000);
        </script>
        </body></html>
        """;
    }

    // ============================================================
    // UTILITIES
    // ============================================================

    static JsonNode get(String url) throws Exception {
        HttpRequest r = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Accept", "application/json")
                .header("User-Agent", "MarketRelationshipLab/1.0")
                .GET().build();

        HttpResponse<String> response = HTTP.send(
                r, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200)
            throw new RuntimeException("HTTP " + response.statusCode());

        return J.readTree(response.body());
    }

    static String s(JsonNode n, String key, String fallback) {
        JsonNode v = n.get(key);
        return v == null || v.isNull() ? fallback : v.asText(fallback);
    }

    static double number(JsonNode n, String key, double fallback) {
        try {
            JsonNode v = n.get(key);
            return v == null || v.isNull() ? fallback : v.asDouble(fallback);
        } catch (Exception e) { return fallback; }
    }

    static void log(String message) {
        String line = Instant.now() + " " + message;
        System.out.println(line);

        try (Connection c = db(); PreparedStatement p =
                c.prepareStatement("INSERT INTO events(message,created) VALUES(?,?)")) {
            p.setString(1, message);
            p.setLong(2, System.currentTimeMillis());
            p.executeUpdate();
        } catch (Exception ignored) {}
    }
}
