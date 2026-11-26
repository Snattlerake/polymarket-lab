package lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static lab.Models.Market;

public final class MarketSources {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(15))
        .build();

    private MarketSources() {}

    private static JsonNode getJson(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .header("User-Agent", "PolymarketResearchLab/2.0")
            .header("Accept", "application/json")
            .GET()
            .build();

        HttpResponse<String> response =
            HTTP.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException(
                "HTTP " + response.statusCode() + " from " + url
            );
        }

        return JSON.readTree(response.body());
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull()
            ? null : value.asText();
    }

    private static Double number(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) return null;
        try {
            return value.isNumber()
                ? value.asDouble()
                : Double.parseDouble(value.asText());
        } catch (Exception ignored) {
            return null;
        }
    }

    public static List<Market> fetchPolymarket(int limit) throws Exception {
        String url = "https://gamma-api.polymarket.com/markets"
            + "?active=true&closed=false&limit=" + limit;

        JsonNode root = getJson(url);
        List<Market> markets = new ArrayList<>();

        if (!root.isArray()) return markets;

        for (JsonNode item : root) {
            String id = text(item, "id");
            String question = text(item, "question");
            if (id == null || question == null) continue;

            Double price = null;
            JsonNode prices = item.path("outcomePrices");

            if (prices.isTextual()) {
                try {
                    JsonNode parsed = JSON.readTree(prices.asText());
                    if (parsed.isArray() && !parsed.isEmpty()) {
                        price = parsed.get(0).asDouble();
                    }
                } catch (Exception ignored) {}
            } else if (prices.isArray() && !prices.isEmpty()) {
                price = prices.get(0).asDouble();
            }

            String marketUrl = text(item, "slug");
            if (marketUrl != null) {
                marketUrl = "https://polymarket.com/event/" + marketUrl;
            }

            markets.add(new Market(
                "POLYMARKET:" + id,
                "POLYMARKET",
                id,
                question,
                marketUrl,
                text(item, "endDate"),
                price,
                "USD",
                item.toString(),
                Instant.now()
            ));
        }

        return markets;
    }

    public static List<Market> fetchKalshi(int limit) throws Exception {
        String url = "https://api.elections.kalshi.com/trade-api/v2/markets"
            + "?status=open&limit=" + limit;

        JsonNode root = getJson(url).path("markets");
        List<Market> markets = new ArrayList<>();

        if (!root.isArray()) return markets;

        for (JsonNode item : root) {
            String id = text(item, "ticker");
            String question = text(item, "title");
            if (question == null) question = text(item, "subtitle");
            if (id == null || question == null) continue;

            Double price = number(item, "yes_ask_dollars");
            if (price == null) {
                price = number(item, "last_price_dollars");
            }
            if (price == null) {
                Double cents = number(item, "yes_ask");
                if (cents != null) price = cents / 100.0;
            }

            String marketUrl = "https://kalshi.com/markets/" + id;

            markets.add(new Market(
                "KALSHI:" + id,
                "KALSHI",
                id,
                question,
                marketUrl,
                text(item, "close_time"),
                price,
                "USD",
                item.toString(),
                Instant.now()
            ));
        }

        return markets;
    }
}
