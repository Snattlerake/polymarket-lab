package lab;

import java.time.Instant;

public final class Models {
    private Models() {}

    public record Market(
        String marketKey,
        String venue,
        String marketId,
        String question,
        String url,
        String endDate,
        Double price,
        String currency,
        String rawJson,
        Instant fetchedAt
    ) {}

    public enum RelationshipType {
        UNCLASSIFIED,
        SAME,
        DEPENDENT,
        UNRELATED
    }

    public record Relationship(
        String marketA,
        String marketB,
        RelationshipType type,
        String dependentMarket,
        String prerequisiteMarket,
        Double confidence,
        String explanation
    ) {}

    public record PairCandidate(
        String marketA,
        String marketB,
        String createdAt
    ) {}
}
