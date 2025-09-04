package com.fix.performance;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fix.performance.metrics.HistogramUtil;
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
    private final Map<String, Message> clOrdIdToMessage;
    private final DataDictionary dictionary;
    private final Recorder recorder = new Recorder(10_000_000_000L, 3);

    public QuickFIXJConsumer(int threadCount) {
        if (threadCount <= 0)
            throw new IllegalArgumentException("threadCount must be > 0");
        this.stripes = new ExecutorService[threadCount];
        for (int i = 0; i < threadCount; i++) {
            this.stripes[i] = Executors.newSingleThreadExecutor();
        }
        this.clOrdIdToMessage = new ConcurrentHashMap<>();
        // Use built-in FIX44 dictionary from QFJ
        try {
            this.dictionary = new DataDictionary("FIX44.xml");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load FIX44 dictionary", e);
        }
    }

    public Map<String, Message> getOpenOrdersMap() {
        return clOrdIdToMessage;
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
                clOrdIdToMessage.put(cl, msg);
            } else if (MsgType.ORDER_CANCEL_REQUEST.equals(msgType)) {
                String orig = msg.getString(41); // OrigClOrdID
                clOrdIdToMessage.remove(orig);
            }
        } catch (Exception e) {
            logger.error("Failed to apply message", e);
        }
    }

    private void processMessageWithTiming(Message msg) {
        final long startNs = System.nanoTime();
        try {
            applyMessage(msg);
        } finally {
            final long endNs = System.nanoTime();
            recorder.recordValue(endNs - startNs);
        }
    }

    @Override
    public void close() {
        shutdownStripes();
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


