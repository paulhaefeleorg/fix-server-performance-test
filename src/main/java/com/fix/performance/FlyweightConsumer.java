package com.fix.performance;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fix.performance.flyweight.Order;
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

    private final Map<Long, Order> clOrdIdToOrder = new ConcurrentHashMap<>();
    // Reusable scratch to minimize garbage while creating small Strings (e.g., ClOrdID, Symbol)
    private final StringBuilder scratch = new StringBuilder(64);
    // Reusable ranges to avoid per-call allocations
    private final MutableRange r35 = new MutableRange();
    private final MutableRange r11 = new MutableRange();
    private final MutableRange r55 = new MutableRange();
    private final MutableRange r38 = new MutableRange();
    private final MutableRange r44 = new MutableRange();
    private final MutableRange r41 = new MutableRange();

    public Map<Long, Order> getOpenOrdersMap() {
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
        long pos = bytes.readPosition();
        long limit = pos + bytes.readRemaining();
        BytesStore<?, ?> store = bytes.bytesStore();

        byte msgType = 0;
        boolean haveMsgType = false;
        long clOrdId = Long.MIN_VALUE;
        long origClOrdId = Long.MIN_VALUE;
        int quantity = Integer.MIN_VALUE;
        long priceCents = Long.MIN_VALUE;
        long symStart = -1;
        long symEnd = -1;

        while (pos < limit) {
            // Parse tag number until '='; skip invalid tokens to next SOH
            int tag = 0;
            boolean hasDigit = false;
            while (pos < limit) {
                int b = store.readUnsignedByte(pos++);
                if (b == '=')
                    break;
                int d = b - '0';
                if (d >= 0 && d <= 9) {
                    tag = tag * 10 + d;
                    hasDigit = true;
                } else {
                    // Skip to end of this field
                    while (pos < limit && store.readUnsignedByte(pos++) != 1) {}
                    hasDigit = false;
                    break;
                }
            }

            long valStart = pos;
            while (pos < limit && store.readUnsignedByte(pos) != 1)
                pos++;
            long valEnd = pos;
            if (pos < limit)
                pos++; // skip SOH

            if (!hasDigit)
                continue;

            switch (tag) {
                case 35: // MsgType
                    if (valEnd > valStart) {
                        msgType = (byte) store.readUnsignedByte(valStart);
                        haveMsgType = true;
                    }
                    break;
                case 11: // ClOrdID
                    clOrdId = parseLong(store, valStart, valEnd);
                    break;
                case 41: // OrigClOrdID
                    origClOrdId = parseLong(store, valStart, valEnd);
                    break;
                case 55: // Symbol
                    symStart = valStart;
                    symEnd = valEnd;
                    break;
                case 38: // OrderQty
                    quantity = parseInt(store, valStart, valEnd);
                    break;
                case 44: // Price
                    priceCents = parsePriceCents(store, valStart, valEnd);
                    break;
                default:
                    break;
            }

            // Early exit when we have all required fields for the message type
            if (haveMsgType) {
                if (msgType == 'D' && clOrdId != Long.MIN_VALUE && symStart != -1
                        && quantity != Integer.MIN_VALUE && priceCents != Long.MIN_VALUE) {
                    break;
                }
                if (msgType == 'F' && origClOrdId != Long.MIN_VALUE) {
                    break;
                }
            }
        }

        if (!haveMsgType)
            return;

        if (msgType == 'D') {
            if (clOrdId == Long.MIN_VALUE || symStart == -1 || quantity == Integer.MIN_VALUE
                    || priceCents == Long.MIN_VALUE)
                return;
            String sym = dedupeSymbol(asciiSlice(store, symStart, symEnd));
            Order ord = acquireOrder();
            ord.set(sym, quantity, priceCents);
            clOrdIdToOrder.put(clOrdId, ord);
        } else if (msgType == 'F') {
            if (origClOrdId == Long.MIN_VALUE)
                return;
            Order removed = clOrdIdToOrder.remove(origClOrdId);
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

    private final java.util.ArrayDeque<Order> pool = new java.util.ArrayDeque<>(1024);
    private final java.util.concurrent.ConcurrentHashMap<String, String> symbolCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    private Order acquireOrder() {
        Order ord = pool.pollFirst();
        return ord != null ? ord : new Order();
    }

    private void releaseOrder(Order ord) {
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

    private static long parseLong(BytesStore<?, ?> store, long s, long e) {
        long v = 0;
        boolean neg = false;
        for (long i = s; i < e; i++) {
            int b = store.readUnsignedByte(i);
            if (b == '-') {
                neg = true;
                continue;
            }
            int d = b - '0';
            if (d >= 0 && d <= 9)
                v = v * 10 + d;
        }
        return neg ? -v : v;
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


