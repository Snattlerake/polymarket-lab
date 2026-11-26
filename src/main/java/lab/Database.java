package lab;

import java.sql.*;
import java.nio.file.*;
import java.util.*;

import static lab.Models.*;

public class Database {
    private final String url;

    public Database(String file) throws Exception {
        Path path = Paths.get(file).toAbsolutePath();
        Files.createDirectories(path.getParent());
        this.url = "jdbc:sqlite:" + path;
        initialize();
    }

    public static void deleteOldDatabase(String file) throws Exception {
        Path path = Paths.get(file).toAbsolutePath();

        Files.deleteIfExists(path);
        Files.deleteIfExists(Path.of(file + "-wal"));
        Files.deleteIfExists(Path.of(file + "-shm"));
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(url);
    }

    private void initialize() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("PRAGMA foreign_keys=ON");

            s.execute("""
                CREATE TABLE IF NOT EXISTS markets (
                    market_key TEXT PRIMARY KEY,
                    venue TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    url TEXT,
                    end_date TEXT,
                    price REAL,
                    currency TEXT,
                    raw_json TEXT,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                )
            """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS market_pairs (
                    market_a TEXT NOT NULL,
                    market_b TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
                    created_at TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    PRIMARY KEY (market_a, market_b),
                    FOREIGN KEY (market_a) REFERENCES markets(market_key),
                    FOREIGN KEY (market_b) REFERENCES markets(market_key)
                )
            """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS relationships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_a TEXT NOT NULL,
                    market_b TEXT NOT NULL,
                    relationship_type TEXT NOT NULL,
                    dependent_market TEXT,
                    prerequisite_market TEXT,
                    confidence REAL,
                    explanation TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (market_a, market_b),
                    FOREIGN KEY (market_a) REFERENCES markets(market_key),
                    FOREIGN KEY (market_b) REFERENCES markets(market_key)
                )
            """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    polymarket_count INTEGER DEFAULT 0,
                    kalshi_count INTEGER DEFAULT 0,
                    total_count INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    error TEXT
                )
            """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL
                )
            """);

            s.execute("CREATE INDEX IF NOT EXISTS idx_markets_venue ON markets(venue)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_markets_last_seen ON markets(last_seen)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_pairs_status ON market_pairs(status)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)");
        }
    }

    public synchronized void saveMarkets(List<Market> markets) throws Exception {
        String sql = """
            INSERT INTO markets (
                market_key, venue, market_id, question, url, end_date,
                price, currency, raw_json, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_key) DO UPDATE SET
                question=excluded.question,
                url=excluded.url,
                end_date=excluded.end_date,
                price=excluded.price,
                currency=excluded.currency,
                raw_json=excluded.raw_json,
                last_seen=excluded.last_seen
        """;

        try (Connection c = connect();
             PreparedStatement p = c.prepareStatement(sql)) {
            c.setAutoCommit(false);

            for (Market m : markets) {
                String now = m.fetchedAt().toString();
                p.setString(1, m.marketKey());
                p.setString(2, m.venue());
                p.setString(3, m.marketId());
                p.setString(4, m.question());
                p.setString(5, m.url());
                p.setString(6, m.endDate());

                if (m.price() == null) p.setNull(7, Types.REAL);
                else p.setDouble(7, m.price());

                p.setString(8, m.currency());
                p.setString(9, m.rawJson());
                p.setString(10, now);
                p.setString(11, now);
                p.addBatch();
            }

            p.executeBatch();
            c.commit();
        }
    }

    public synchronized void createCrossVenuePairs(List<Market> scanned) throws Exception {
        String sql = """
            INSERT INTO market_pairs (market_a, market_b, status, created_at, last_seen)
            VALUES (?, ?, 'UNCLASSIFIED', ?, ?)
            ON CONFLICT(market_a, market_b)
            DO UPDATE SET last_seen=excluded.last_seen
        """;

        List<String[]> keys = new ArrayList<>();

        try (Connection c = connect();
             PreparedStatement q = c.prepareStatement(
                 "SELECT market_key, venue FROM markets WHERE venue <> ?"
             );
             PreparedStatement p = c.prepareStatement(sql)) {

            c.setAutoCommit(false);

            for (Market m : scanned) {
                q.setString(1, m.venue());

                try (ResultSet rs = q.executeQuery()) {
                    while (rs.next()) {
                        String other = rs.getString("market_key");
                        String a = m.marketKey().compareTo(other) < 0
                            ? m.marketKey() : other;
                        String b = m.marketKey().compareTo(other) < 0
                            ? other : m.marketKey();

                        keys.add(new String[]{a, b});
                    }
                }
            }

            String now = java.time.Instant.now().toString();
            for (String[] pair : keys) {
                p.setString(1, pair[0]);
                p.setString(2, pair[1]);
                p.setString(3, now);
                p.setString(4, now);
                p.addBatch();
            }

            p.executeBatch();
            c.commit();
        }
    }

    public synchronized void saveRelationship(Relationship r) throws Exception {
        if (r.type() == RelationshipType.DEPENDENT) {
            if (r.dependentMarket() == null || r.prerequisiteMarket() == null) {
                throw new IllegalArgumentException(
                    "DEPENDENT requires dependentMarket and prerequisiteMarket"
                );
            }
        }

        String a = r.marketA().compareTo(r.marketB()) < 0
            ? r.marketA() : r.marketB();
        String b = r.marketA().compareTo(r.marketB()) < 0
            ? r.marketB() : r.marketA();

        String sql = """
            INSERT INTO relationships (
                market_a, market_b, relationship_type, dependent_market,
                prerequisite_market, confidence, explanation, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(market_a, market_b) DO UPDATE SET
                relationship_type=excluded.relationship_type,
                dependent_market=excluded.dependent_market,
                prerequisite_market=excluded.prerequisite_market,
                confidence=excluded.confidence,
                explanation=excluded.explanation,
                updated_at=CURRENT_TIMESTAMP
        """;

        try (Connection c = connect();
             PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, a);
            p.setString(2, b);
            p.setString(3, r.type().name());
            p.setString(4, r.dependentMarket());
            p.setString(5, r.prerequisiteMarket());

            if (r.confidence() == null) p.setNull(6, Types.REAL);
            else p.setDouble(6, r.confidence());

            p.setString(7, r.explanation());
            p.executeUpdate();
        }
    }

    public synchronized long startScan() throws Exception {
        try (Connection c = connect();
             PreparedStatement p = c.prepareStatement(
                 "INSERT INTO scan_runs(started_at,status) VALUES(?, 'RUNNING')",
                 Statement.RETURN_GENERATED_KEYS
             )) {
            p.setString(1, java.time.Instant.now().toString());
            p.executeUpdate();

            try (ResultSet rs = p.getGeneratedKeys()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }

    public synchronized void finishScan(
        long id, int poly, int kalshi, String error
    ) throws Exception {
        try (Connection c = connect();
             PreparedStatement p = c.prepareStatement("""
                 UPDATE scan_runs SET finished_at=?, polymarket_count=?,
                 kalshi_count=?, total_count=?, status=?, error=? WHERE id=?
             """)) {
            p.setString(1, java.time.Instant.now().toString());
            p.setInt(2, poly);
            p.setInt(3, kalshi);
            p.setInt(4, poly + kalshi);
            p.setString(5, error == null ? "COMPLETED" : "FAILED");
            p.setString(6, error);
            p.setLong(7, id);
            p.executeUpdate();
        }
    }

    public synchronized void event(String level, String message) throws Exception {
        try (Connection c = connect();
             PreparedStatement p = c.prepareStatement(
                 "INSERT INTO events(created_at,level,message) VALUES(?,?,?)"
             )) {
            p.setString(1, java.time.Instant.now().toString());
            p.setString(2, level);
            p.setString(3, message);
            p.executeUpdate();
        }
    }

    private List<Map<String, Object>> query(String sql) throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();

        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    row.put(md.getColumnLabel(i), rs.getObject(i));
                }
                rows.add(row);
            }
        }

        return rows;
    }

    public List<Map<String, Object>> markets() throws Exception {
        return query("""
            SELECT * FROM markets ORDER BY last_seen DESC LIMIT 1000
        """);
    }

    public List<Map<String, Object>> pairs() throws Exception {
        return query("""
            SELECT p.market_a, p.market_b, p.status,
                   a.question AS question_a, a.venue AS venue_a, a.url AS url_a,
                   a.price AS price_a, a.currency AS currency_a,
                   b.question AS question_b, b.venue AS venue_b, b.url AS url_b,
                   b.price AS price_b, b.currency AS currency_b,
                   p.created_at, p.last_seen
            FROM market_pairs p
            JOIN markets a ON a.market_key=p.market_a
            JOIN markets b ON b.market_key=p.market_b
            ORDER BY p.last_seen DESC LIMIT 1000
        """);
    }

    public List<Map<String, Object>> relationships() throws Exception {
        return query("SELECT * FROM relationships ORDER BY updated_at DESC LIMIT 1000");
    }

    public List<Map<String, Object>> events() throws Exception {
        return query("SELECT * FROM events ORDER BY id DESC LIMIT 200");
    }

    public Map<String, Object> stats() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            result.put("markets", count(s, "markets"));
            result.put("pairs", count(s, "market_pairs"));
            result.put("relationships", count(s, "relationships"));
            result.put("scans", count(s, "scan_runs"));
            result.put("events", count(s, "events"));
        }
        return result;
    }

    private long count(Statement s, String table) throws SQLException {
        try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    public List<Map<String, Object>> scans() throws Exception {
        return query("SELECT * FROM scan_runs ORDER BY id DESC LIMIT 50");
    }
}
