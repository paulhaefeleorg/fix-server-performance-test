package com.fix.performance;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Recorder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fix.performance.metrics.HistogramUtil;
import com.fix.performance.flyweight.Order;
import com.fix.performance.metrics.GcTracker;
import com.fix.performance.queue.ChronicleQueueService;
import quickfix.DataDictionary;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.MsgType;

/**
 * Consumes FIX strings from Chronicle Queue, parses to QuickFIX/J Message, and dispatches work on a
 * thread pool to maintain a map of open orders by ClOrdID. NewOrderSingle stores the message;
 * OrderCancelRequest removes the original order.
 */
public final class QuickFIXJConsumer implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(QuickFIXJConsumer.class);

    private final ExecutorService[] stripes;
    private final Map<String, Order> clOrdIdToOrder;
    private final DataDictionary dictionary;
    private final Recorder recorder = new Recorder(10_000_000_000L, 3);
    private static final int WARMUP_SKIP = 100;
    private final AtomicLong processedCounter = new AtomicLong(0);
    final GcTracker gcTracker = new GcTracker().start();

    public QuickFIXJConsumer(int threadCount) {
        if (threadCount <= 0)
            throw new IllegalArgumentException("threadCount must be > 0");
        this.stripes = new ExecutorService[threadCount];
        for (int i = 0; i < threadCount; i++) {
            this.stripes[i] = Executors.newSingleThreadExecutor();
        }
        this.clOrdIdToOrder = new ConcurrentHashMap<>();
        // Use built-in FIX44 dictionary from QFJ
        try {
            this.dictionary = new DataDictionary("FIX44.xml");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load FIX44 dictionary", e);
        }
    }

    public Map<String, Order> getOpenOrdersMap() {
        return clOrdIdToOrder;
    }

    public void consume(Path queuePath) {
        Objects.requireNonNull(queuePath, "queuePath");
        try (ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            svc.forEach(this::submitWork);
        }
    }

    public void consume(Path queuePath, Path metricsOut) {
        consume(queuePath);
        shutdownStripes();
        HistogramUtil.writeHistogram(metricsOut, recorder, "QuickFIXJ");
    }

    private void submitWork(String fixString) {
        // Parse once to route to a per-key single-thread stripe ensuring ordering per ClOrdID
        try {
            Message msg = new Message();
            msg.fromString(fixString, dictionary, false);
            String msgType = msg.getHeader().getString(MsgType.FIELD);
            String key;
            if (MsgType.ORDER_SINGLE.equals(msgType)) {
                key = msg.getString(ClOrdID.FIELD);
            } else if (MsgType.ORDER_CANCEL_REQUEST.equals(msgType)) {
                key = msg.getString(41); // OrigClOrdID
            } else {
                return;
            }
            int idx = Math.floorMod(key.hashCode(), stripes.length);
            stripes[idx].submit(() -> processMessageWithTiming(msg));
        } catch (Exception e) {
            logger.error("Failed to route FIX: {}", fixString, e);
        }
    }

    private void applyMessage(Message msg) {
        try {
            String msgType = msg.getHeader().getString(MsgType.FIELD);
            if (MsgType.ORDER_SINGLE.equals(msgType)) {
                String cl = msg.getString(ClOrdID.FIELD);
                Order ord = convertToOrder(msg);
                clOrdIdToOrder.put(cl, ord);
            } else if (MsgType.ORDER_CANCEL_REQUEST.equals(msgType)) {
                String orig = msg.getString(41); // OrigClOrdID
                clOrdIdToOrder.remove(orig);
            }
        } catch (Exception e) {
            logger.error("Failed to apply message", e);
        }
    }

    private static Order convertToOrder(Message msg) throws Exception {
        String symbol = msg.getString(55);
        int qty = msg.getInt(38);
        // Price may be decimal; convert to cents similar to flyweight
        long priceCents = parsePriceCents(msg.getString(44));
        Order ord = new Order();
        ord.set(symbol, qty, priceCents);
        return ord;
    }

    private static long parsePriceCents(String priceStr) {
        long dollars = 0;
        long cents = 0;
        boolean frac = false;
        int fracDigits = 0;
        boolean neg = false;
        for (int i = 0; i < priceStr.length(); i++) {
            char c = priceStr.charAt(i);
            if (c == '-') { neg = true; continue; }
            if (c == '.') { frac = true; continue; }
            int d = c - '0';
            if (d < 0 || d > 9) continue;
            if (!frac) dollars = dollars * 10 + d; else if (fracDigits < 2) { cents = cents * 10 + d; fracDigits++; }
        }
        while (fracDigits < 2) { cents *= 10; fracDigits++; }
        long total = dollars * 100 + cents;
        return neg ? -total : total;
    }

    private void processMessageWithTiming(Message msg) {
        final long startNs = System.nanoTime();
        try {
            applyMessage(msg);
        } finally {
            final long endNs = System.nanoTime();
            long prev = processedCounter.getAndIncrement();
            if (prev >= WARMUP_SKIP) {
                recorder.recordValue(endNs - startNs);
            }
        }
    }

    @Override
    public void close() {
        shutdownStripes();
        gcTracker.stop();
    }

    private void shutdownStripes() {
        for (ExecutorService es : stripes)
            es.shutdown();
        for (ExecutorService es : stripes) {
            try {
                es.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}


