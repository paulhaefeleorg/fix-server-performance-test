package com.fix.performance.generator;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import com.fix.performance.fix.FixMessageBuilder;
import com.fix.performance.queue.ChronicleQueueService;

/**
 * Generates FIX 4.4 messages (NewOrderSingle + OrderCancelRequest) and writes them to Chronicle
 * Queue. Cancels are scheduled to occur within the next 100 messages relative to the originating
 * NOS when possible. After reaching the requested message budget, remaining outstanding cancels are
 * flushed.
 */
public final class FixMessageGenerator {
    private static final String[] SYMBOLS = {"AAPL", "MSFT", "GOOGL", "AMZN", "META"};

    public GenerationResult generate(Path queuePath, long requestedMessages, long randomSeed,
            String senderCompId, String targetCompId) {
        Objects.requireNonNull(queuePath, "queuePath");
        if (requestedMessages <= 0)
            throw new IllegalArgumentException("requestedMessages must be > 0");

        FixMessageBuilder builder = new FixMessageBuilder(senderCompId, targetCompId);
        Random random = new Random(randomSeed);
        Map<String, OrderInfo> outstanding = new HashMap<>();
        long nosCount = 0;
        long cancelCount = 0;

        // Base price per symbol, within pennies
        Map<String, Long> basePriceCents = new HashMap<>();
        for (String s : SYMBOLS) {
            long dollars = 100 + random.nextInt(200); // 100..299 dollars
            long cents = random.nextInt(100);
            basePriceCents.put(s, dollars * 100 + cents);
        }

        // Min-heap of scheduled cancels by dueIndex
        PriorityQueue<ScheduledCancel> dueCancels =
                new PriorityQueue<>(Comparator.comparingLong(sc -> sc.dueIndex));

        long index = 0;
        long produced = 0;

        try (ChronicleQueueService queue = new ChronicleQueueService(queuePath)) {
            while (produced < requestedMessages) {
                // Emit all due cancels first if any are due at or before current index
                while (produced < requestedMessages && !dueCancels.isEmpty()
                        && dueCancels.peek().dueIndex <= index) {
                    ScheduledCancel sc = dueCancels.poll();
                    OrderInfo info = outstanding.remove(sc.clOrdId);
                    if (info == null) {
                        continue;
                    }
                    String oc = builder.buildOrderCancelRequest(nextClOrdId(cancelCount + 1),
                            info.clOrdId, info.symbol, info.side, System.nanoTime());
                    queue.writeFix(oc);
                    cancelCount++;
                    produced++;
                    index++;
                }

                if (produced >= requestedMessages)
                    break;

                long remaining = requestedMessages - produced;
                // In the last 100 messages, prioritize emitting cancels so that all first-phase
                // NOS receive their cancel within 100 messages
                if (remaining <= 100 && !outstanding.isEmpty()) {
                    Map.Entry<String, OrderInfo> e = outstanding.entrySet().iterator().next();
                    OrderInfo info = e.getValue();
                    outstanding.remove(info.clOrdId);
                    String oc = builder.buildOrderCancelRequest(nextClOrdId(cancelCount + 1),
                            info.clOrdId, info.symbol, info.side, System.nanoTime());
                    queue.writeFix(oc);
                    cancelCount++;
                    produced++;
                    index++;
                    continue;
                }

                // Otherwise emit a new NOS
                String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];
                char side = random.nextBoolean() ? '1' : '2'; // 1=Buy, 2=Sell
                int qty = (random.nextInt(10) + 1) * 100; // 100..1000

                long base = basePriceCents.get(symbol);
                int offset = random.nextInt(21) - 10; // -10..+10 cents around base
                long priceCents = base + offset;

                String clOrdId = nextClOrdId(nosCount + 1);
                String nos = builder.buildNewOrderSingle(clOrdId, symbol, side, qty, priceCents,
                        System.nanoTime());
                queue.writeFix(nos);
                nosCount++;
                produced++;

                // Track outstanding and schedule cancel within next 100 messages
                outstanding.put(clOrdId, new OrderInfo(clOrdId, symbol, side));
                long due = index + 1 + random.nextInt(100); // within next 100 messages
                dueCancels.add(new ScheduledCancel(clOrdId, due));

                index++;
            }

            long firstPhaseMessages = produced;

            // Flush remaining cancels for outstanding NOS
            while (!outstanding.isEmpty()) {
                Map.Entry<String, OrderInfo> e = outstanding.entrySet().iterator().next();
                OrderInfo info = e.getValue();
                outstanding.remove(info.clOrdId);

                String oc = builder.buildOrderCancelRequest(nextClOrdId(cancelCount + 1),
                        info.clOrdId, info.symbol, info.side, System.nanoTime());
                queue.writeFix(oc);
                cancelCount++;
                produced++;
            }

            return new GenerationResult(produced, nosCount, cancelCount, firstPhaseMessages);
        }
    }

    private static String nextClOrdId(long id) {
        return Long.toString(id);
    }

    private record OrderInfo(String clOrdId, String symbol, char side) {
    }

    private record ScheduledCancel(String clOrdId, long dueIndex) {
    }
}


