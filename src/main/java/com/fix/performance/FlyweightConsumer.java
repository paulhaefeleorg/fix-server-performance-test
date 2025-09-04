package com.fix.performance;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fix.performance.flyweight.FlyweightOrder;
import com.fix.performance.metrics.HistogramUtil;
import com.fix.performance.queue.ChronicleQueueService;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;

/**
 * Flyweight consumer: parses FIX strings directly (string scanning) to extract fields without
 * building full message objects. Maintains a map of open orders keyed by ClOrdID.
 */
public final class FlyweightConsumer implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(FlyweightConsumer.class);

    private final Map<Long, FlyweightOrder> clOrdIdToOrder = new ConcurrentHashMap<>();
    // Reusable scratch to minimize garbage while creating small Strings (e.g., ClOrdID, Symbol)
    private final StringBuilder scratch = new StringBuilder(64);
    // Reusable ranges to avoid per-call allocations
    private final MutableRange r35 = new MutableRange();
    private final MutableRange r11 = new MutableRange();
    private final MutableRange r55 = new MutableRange();
    private final MutableRange r38 = new MutableRange();
    private final MutableRange r44 = new MutableRange();
    private final MutableRange r41 = new MutableRange();

    public Map<Long, FlyweightOrder> getOpenOrdersMap() {
        return clOrdIdToOrder;
    }

    // Metrics recording (ns)
    private final org.HdrHistogram.Recorder recorder =
            new org.HdrHistogram.Recorder(10_000_000_000L, 3);

    public void consume(Path queuePath) {
        Objects.requireNonNull(queuePath, "queuePath");
        try (AffinityLock lock = AffinityLock.acquireLock();
                ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            svc.forEachBytes(this::processBytesWithTiming);
        }
    }

    public void consume(Path queuePath, Path metricsOut) {
        Objects.requireNonNull(queuePath, "queuePath");
        try (AffinityLock lock = AffinityLock.acquireLock();
                ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            svc.forEachBytes(this::processBytesWithTiming);
        }
        HistogramUtil.writeHistogram(metricsOut, recorder, "Flyweight");
    }

    void processBytes(Bytes<?> bytes) {
        long start = bytes.readPosition();
        long limit = start + bytes.readRemaining();
        BytesStore<?, ?> store = bytes.bytesStore();

        if (!findTag(store, start, limit, 35, r35))
            return;
        byte mt = store.readByte(r35.start);
        if (mt == 'D') {
            if (!findTag(store, start, limit, 11, r11) || !findTag(store, start, limit, 55, r55)
                    || !findTag(store, start, limit, 38, r38)
                    || !findTag(store, start, limit, 44, r44))
                return;
            String clStr = asciiSlice(store, r11.start, r11.end);
            long cl = parseLongAscii(clStr);
            String sym = dedupeSymbol(asciiSlice(store, r55.start, r55.end));
            int qty = parseInt(store, r38.start, r38.end);
            long priceCents = parsePriceCents(store, r44.start, r44.end);
            FlyweightOrder ord = acquireOrder();
            ord.set(sym, qty, priceCents);
            clOrdIdToOrder.put(cl, ord);
        } else if (mt == 'F') {
            if (!findTag(store, start, limit, 41, r41))
                return;
            long orig = parseLongAscii(asciiSlice(store, r41.start, r41.end));
            FlyweightOrder removed = clOrdIdToOrder.remove(orig);
            if (removed != null)
                releaseOrder(removed);
        }
    }

    private void processBytesWithTiming(Bytes<?> bytes) {
        final long startNs = System.nanoTime();
        try {
            processBytes(bytes);
        } finally {
            final long endNs = System.nanoTime();
            recorder.recordValue(endNs - startNs);
        }
    }

    private final java.util.ArrayDeque<FlyweightOrder> pool = new java.util.ArrayDeque<>(1024);
    private final java.util.concurrent.ConcurrentHashMap<String, String> symbolCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    private FlyweightOrder acquireOrder() {
        FlyweightOrder ord = pool.pollFirst();
        return ord != null ? ord : new FlyweightOrder();
    }

    private void releaseOrder(FlyweightOrder ord) {
        // Optionally clear fields if needed; keep it simple to reduce cost
        if (pool.size() < 8192)
            pool.addFirst(ord);
    }

    private String dedupeSymbol(String s) {
        String cached = symbolCache.putIfAbsent(s, s);
        return cached == null ? s : cached;
    }

    private static long parseLongAscii(String s) {
        long v = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9')
                break;
            v = v * 10 + (c - '0');
        }
        return v;
    }

    private String asciiSlice(BytesStore<?, ?> store, long start, long end) {
        scratch.setLength(0);
        for (long i = start; i < end; i++) {
            scratch.append((char) store.readUnsignedByte(i));
        }
        return scratch.toString();
    }

    private static int parseInt(BytesStore<?, ?> store, long s, long e) {
        int val = 0;
        for (long i = s; i < e; i++) {
            int d = store.readUnsignedByte(i) - '0';
            if (d >= 0 && d <= 9)
                val = val * 10 + d;
        }
        return val;
    }

    private static long parsePriceCents(BytesStore<?, ?> store, long s, long e) {
        long dollars = 0;
        long cents = 0;
        boolean frac = false;
        int fracDigits = 0;
        boolean neg = false;
        for (long i = s; i < e; i++) {
            int b = store.readUnsignedByte(i);
            if (b == '-') {
                neg = true;
                continue;
            }
            if (b == '.') {
                frac = true;
                continue;
            }
            int d = b - '0';
            if (d < 0 || d > 9)
                continue;
            if (!frac)
                dollars = dollars * 10 + d;
            else if (fracDigits < 2) {
                cents = cents * 10 + d;
                fracDigits++;
            }
        }
        while (fracDigits < 2) {
            cents *= 10;
            fracDigits++;
        }
        long total = dollars * 100 + cents;
        return neg ? -total : total;
    }

    private static boolean findTag(BytesStore<?, ?> store, long start, long limit, int targetTag,
            MutableRange out) {
        long i = start;
        while (i < limit) {
            int tag = 0;
            boolean hasDigit = false;
            long j = i;
            for (; j < limit; j++) {
                int b = store.readUnsignedByte(j);
                if (b == '=')
                    break;
                int d = b - '0';
                if (d >= 0 && d <= 9) {
                    tag = tag * 10 + d;
                    hasDigit = true;
                } else {
                    while (j < limit && store.readUnsignedByte(j) != 1)
                        j++;
                    break;
                }
            }
            if (j >= limit)
                return false;
            long valStart = j + 1;
            long k = valStart;
            while (k < limit && store.readUnsignedByte(k) != 1)
                k++;
            if (hasDigit && tag == targetTag) {
                out.start = valStart;
                out.end = k;
                return true;
            }
            i = k + 1;
        }
        return false;
    }

    private static final class MutableRange {
        long start;
        long end;
    }

    // Removed string-based helpers; flyweight operates on Bytes directly

    @Override
    public void close() {
        // nothing
    }
}


