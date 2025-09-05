package com.fix.performance.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import com.sun.management.GarbageCollectionNotificationInfo;

/**
 * Tracks GC pause events and total pause time using JMX notifications.
 */
public final class GcTracker implements AutoCloseable, NotificationListener {
    private final List<NotificationEmitter> emitters = new ArrayList<>();
    private final AtomicLong pauseCount = new AtomicLong();
    private final AtomicLong totalPauseMs = new AtomicLong();
    private final ConcurrentHashMap<String, LongAdder> countByType = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> totalMsByType = new ConcurrentHashMap<>();
    private volatile boolean started = false;
    private final List<MemoryPoolMXBean> memoryPools = new ArrayList<>();
    private final ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicLong> maxUsedBytesByPool =
            new ConcurrentHashMap<>();

    public GcTracker start() {
        if (started)
            return this;
        for (GarbageCollectorMXBean mx : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (mx instanceof NotificationEmitter ne) {
                ne.addNotificationListener(this, null, null);
                emitters.add(ne);
            }
        }
        // Capture memory pools and reset their peaks
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.isValid()) {
                memoryPools.add(pool);
                try {
                    pool.resetPeakUsage();
                } catch (Exception ignored) {
                }
            }
        }
        started = true;
        return this;
    }

    public Snapshot snapshot() {
        Map<String, Long> counts = new HashMap<>();
        for (Map.Entry<String, LongAdder> e : countByType.entrySet()) {
            counts.put(e.getKey(), e.getValue().longValue());
        }
        Map<String, Long> totals = new HashMap<>();
        for (Map.Entry<String, LongAdder> e : totalMsByType.entrySet()) {
            totals.put(e.getKey(), e.getValue().longValue());
        }
        // Compose peak usage maps from JMX and our tracked maxima
        Map<String, Long> jmxPeakUsed = new HashMap<>();
        for (MemoryPoolMXBean pool : memoryPools) {
            try {
                MemoryUsage peak = pool.getPeakUsage();
                if (peak != null)
                    jmxPeakUsed.put(pool.getName(), peak.getUsed());
            } catch (Exception ignored) {
            }
        }
        Map<String, Long> trackedMaxUsed = new HashMap<>();
        for (Map.Entry<String, java.util.concurrent.atomic.AtomicLong> e : maxUsedBytesByPool.entrySet()) {
            trackedMaxUsed.put(e.getKey(), e.getValue().get());
        }
        return new Snapshot(pauseCount.get(), totalPauseMs.get(), counts, totals, jmxPeakUsed,
                trackedMaxUsed);
    }

    public Snapshot stop() {
        for (NotificationEmitter ne : emitters) {
            try {
                ne.removeNotificationListener(this);
            } catch (Exception ignored) {
            }
        }
        emitters.clear();
        started = false;
        return snapshot();
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                .equals(notification.getType()))
            return;
        Object userData = notification.getUserData();
        if (!(userData instanceof CompositeData cd))
            return;
        GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
        long duration = info.getGcInfo().getDuration();
        pauseCount.incrementAndGet();
        totalPauseMs.addAndGet(duration);
        String type = info.getGcName() + ":" + info.getGcAction();
        countByType.computeIfAbsent(type, k -> new LongAdder()).increment();
        totalMsByType.computeIfAbsent(type, k -> new LongAdder()).add(duration);
        // Update per-pool max used bytes at this point-in-time
        for (MemoryPoolMXBean pool : memoryPools) {
            try {
                MemoryUsage usage = pool.getUsage();
                if (usage == null)
                    continue;
                long used = usage.getUsed();
                maxUsedBytesByPool.computeIfAbsent(pool.getName(), k -> new java.util.concurrent.atomic.AtomicLong(0))
                        .updateAndGet(prev -> Math.max(prev, used));
            } catch (Exception ignored) {
            }
        }
    }

    public static final class Snapshot {
        public final long count;
        public final long totalPauseMs;
        public final Map<String, Long> countByType;
        public final Map<String, Long> totalPauseMsByType;
        public final Map<String, Long> jmxPeakUsedByPool;
        public final Map<String, Long> trackedMaxUsedByPool;

        public Snapshot(long count, long totalPauseMs, Map<String, Long> countByType,
                Map<String, Long> totalPauseMsByType, Map<String, Long> jmxPeakUsedByPool,
                Map<String, Long> trackedMaxUsedByPool) {
            this.count = count;
            this.totalPauseMs = totalPauseMs;
            this.countByType = java.util.Collections.unmodifiableMap(countByType);
            this.totalPauseMsByType = java.util.Collections.unmodifiableMap(totalPauseMsByType);
            this.jmxPeakUsedByPool = java.util.Collections.unmodifiableMap(jmxPeakUsedByPool);
            this.trackedMaxUsedByPool = java.util.Collections.unmodifiableMap(trackedMaxUsedByPool);
        }
    }
}


