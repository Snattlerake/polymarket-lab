package lab;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static lab.Models.*;

public class ScannerService {
    private final Database db;
    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final int intervalSeconds;
    private final int marketLimit;

    public ScannerService(Database db, int intervalSeconds, int marketLimit) {
        this.db = db;
        this.intervalSeconds = Math.max(10, intervalSeconds);
        this.marketLimit = Math.max(1, marketLimit);
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(
            this::scheduledScan,
            0,
            intervalSeconds,
            TimeUnit.SECONDS
        );
    }

    private void scheduledScan() {
        if (enabled.get()) {
            scanNow();
        }
    }

    public void setEnabled(boolean value) {
        enabled.set(value);
        try {
            db.event("INFO", "Scanning " + (value ? "enabled" : "disabled"));
        } catch (Exception ignored) {}
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public boolean isRunning() {
        return running.get();
    }

    public void scanNow() {
        if (!running.compareAndSet(false, true)) return;

        Thread worker = new Thread(() -> {
            long scanId = -1;
            int polyCount = 0;
            int kalshiCount = 0;
            String error = null;

            try {
                scanId = db.startScan();
                db.event("INFO", "Market scan started");

                List<Market> all = new ArrayList<>();

                try {
                    List<Market> poly =
                        MarketSources.fetchPolymarket(marketLimit);
                    polyCount = poly.size();
                    all.addAll(poly);
                } catch (Exception e) {
                    db.event("ERROR", "Polymarket: " + e.getMessage());
                }

                try {
                    List<Market> kalshi =
                        MarketSources.fetchKalshi(marketLimit);
                    kalshiCount = kalshi.size();
                    all.addAll(kalshi);
                } catch (Exception e) {
                    db.event("ERROR", "Kalshi: " + e.getMessage());
                }

                if (all.isEmpty()) {
                    throw new IllegalStateException(
                        "No markets were returned by either source"
                    );
                }

                db.saveMarkets(all);
                db.createCrossVenuePairs(all);

                // Placeholder call: intentionally returns null.
                // No relationship classification is performed.
                for (int i = 0; i < all.size(); i++) {
                    Market a = all.get(i);
                    for (int j = i + 1; j < all.size(); j++) {
                        Market b = all.get(j);
                        if (!a.venue().equals(b.venue())) {
                            Relationship ignored =
                                RelationshipEvaluator.evaluate(a, b);
                        }
                    }
                }

                db.event(
                    "INFO",
                    "Scan saved " + all.size() + " markets; "
                    + polyCount + " Polymarket and "
                    + kalshiCount + " Kalshi"
                );

            } catch (Exception e) {
                error = e.toString();
                try {
                    db.event("ERROR", "Scan failed: " + error);
                } catch (Exception ignored) {}
            } finally {
                try {
                    if (scanId != -1) {
                        db.finishScan(scanId, polyCount, kalshiCount, error);
                    }
                } catch (Exception ignored) {}
                running.set(false);
            }
        }, "market-scan-worker");

        worker.setDaemon(true);
        worker.start();
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
