package lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static lab.Models.*;

public class Main {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static Database db;
    private static ScannerService scanner;

    public static void main(String[] args) throws Exception {
        String dbFile = env("DB_FILE", "/app/data/lab.db");

        // Set RESET_DATABASE_ON_START=true for the first migration only.
        if (Boolean.parseBoolean(env("RESET_DATABASE_ON_START", "false"))) {
            Database.deleteOldDatabase(dbFile);
            System.out.println("Old SQLite database files deleted.");
        }

        db = new Database(dbFile);

        scanner = new ScannerService(
            db,
            Integer.parseInt(env("SCAN_SECONDS", "90")),
            Integer.parseInt(env("MAX_MARKETS", "500"))
        );
        scanner.start();

        int port = Integer.parseInt(env("PORT", "8080"));
        HttpServer server = HttpServer.create(
            new InetSocketAddress("0.0.0.0", port), 0
        );

        server.createContext("/", Main::handle);
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(8));
        server.start();

        db.event("INFO", "Application started on port " + port);
        System.out.println("Polymarket Research Lab listening on " + port);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        try {
            if (path.equals("/") && method.equals("GET")) {
                serveIndex(exchange);
                return;
            }

            if (path.equals("/health")) {
                sendJson(exchange, Map.of("ok", true));
                return;
            }

            if (path.equals("/api/state")) {
                Map<String, Object> state = new LinkedHashMap<>();
                state.put("enabled", scanner.isEnabled());
                state.put("running", scanner.isRunning());
                state.put("stats", db.stats());
                sendJson(exchange, state);
                return;
            }

            if (path.equals("/api/markets")) {
                sendJson(exchange, db.markets());
                return;
            }

            if (path.equals("/api/pairs")) {
                sendJson(exchange, db.pairs());
                return;
            }

            if (path.equals("/api/relationships")) {
                if (method.equals("GET")) {
                    sendJson(exchange, db.relationships());
                    return;
                }
                if (method.equals("POST")) {
                    saveRelationship(exchange);
                    return;
                }
            }

            if (path.equals("/api/events")) {
                sendJson(exchange, db.events());
                return;
            }

            if (path.equals("/api/scans")) {
                sendJson(exchange, db.scans());
                return;
            }

            if (path.equals("/api/control") && method.equals("POST")) {
                JsonNode body = JSON.readTree(exchange.getRequestBody());
                String action = body.path("action").asText("");

                switch (action) {
                    case "start" -> scanner.setEnabled(true);
                    case "stop" -> scanner.setEnabled(false);
                    case "scan" -> scanner.scanNow();
                    default -> {
                        send(exchange, 400, "Unknown action");
                        return;
                    }
                }

                sendJson(exchange, Map.of(
                    "enabled", scanner.isEnabled(),
                    "running", scanner.isRunning()
                ));
                return;
            }

            send(exchange, 404, "Not found");

        } catch (Exception e) {
            e.printStackTrace();
            send(exchange, 500, e.toString());
        }
    }

    private static void saveRelationship(HttpExchange exchange)
        throws Exception {
        JsonNode body = JSON.readTree(exchange.getRequestBody());

        String a = body.path("marketA").asText(null);
        String b = body.path("marketB").asText(null);
        String type = body.path("type").asText("UNCLASSIFIED");

        if (a == null || b == null || a.equals(b)) {
            send(exchange, 400, "marketA and marketB must be different market keys");
            return;
        }

        RelationshipType relationshipType;
        try {
            relationshipType = RelationshipType.valueOf(type.toUpperCase());
        } catch (Exception e) {
            send(exchange, 400, "Invalid relationship type");
            return;
        }

        String dependent = body.path("dependentMarket").asText(null);
        String prerequisite = body.path("prerequisiteMarket").asText(null);

        if (relationshipType == RelationshipType.DEPENDENT) {
            if (dependent == null || prerequisite == null
                || !(dependent.equals(a) || dependent.equals(b))
                || !(prerequisite.equals(a) || prerequisite.equals(b))
                || dependent.equals(prerequisite)) {
                send(exchange, 400,
                    "DEPENDENT requires one market as dependentMarket and the other as prerequisiteMarket");
                return;
            }
        }

        Double confidence = body.has("confidence")
            && body.get("confidence").isNumber()
            ? body.get("confidence").asDouble() : null;

        db.saveRelationship(new Relationship(
            a, b, relationshipType, dependent, prerequisite,
            confidence, body.path("explanation").asText(null)
        ));

        sendJson(exchange, Map.of("saved", true));
    }

    private static void serveIndex(HttpExchange exchange) throws IOException {
        try (InputStream in = Main.class.getResourceAsStream("/static/index.html")) {
            if (in == null) {
                send(exchange, 500, "Dashboard resource not found");
                return;
            }
            byte[] bytes = in.readAllBytes();
            exchange.getResponseHeaders().set(
                "Content-Type", "text/html; charset=utf-8"
            );
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }

    private static void sendJson(HttpExchange exchange, Object value)
        throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(value);
        exchange.getResponseHeaders().set(
            "Content-Type", "application/json; charset=utf-8"
        );
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static void send(
        HttpExchange exchange, int status, String message
    ) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set(
            "Content-Type", "text/plain; charset=utf-8"
        );
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
