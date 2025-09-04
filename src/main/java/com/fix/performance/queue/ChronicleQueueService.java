package com.fix.performance.queue;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * Minimal Chronicle Queue wrapper for writing/reading FIX messages. Messages are stored as
 * documents with a single text field "fix".
 */
public final class ChronicleQueueService implements Closeable {
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;

    public ChronicleQueueService(Path path) {
        this.queue = SingleChronicleQueueBuilder.binary(path.toString()).build();
        // Use createAppender() for compatibility across Chronicle Queue versions
        this.appender = this.queue.createAppender();
    }

    public void writeFix(String fixMessage) {
        byte[] ascii = fixMessage.getBytes(StandardCharsets.US_ASCII);
        appender.writeDocument(w -> w.write("fix").bytes(ascii));
    }

    public void writeFixBytes(byte[] rawFix) {
        appender.writeDocument(w -> w.write("fix").bytes(rawFix));
    }

    public List<String> readAll() {
        List<String> messages = new ArrayList<>();
        ExcerptTailer tailer = queue.createTailer();
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                BytesStore<?, ?> store = dc.wire().read("fix").bytesStore();
                if (store == null)
                    break;
                messages.add(asciiFrom(store));
            }
        }
        return messages;
    }

    public void forEach(Consumer<String> consumer) {
        ExcerptTailer tailer = queue.createTailer();
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                BytesStore<?, ?> store = dc.wire().read("fix").bytesStore();
                if (store == null)
                    break;
                consumer.accept(asciiFrom(store));
            }
        }
    }

    /**
     * Iterate over raw FIX bytes (zero-copy) for flyweight consumers. Bytes is valid only within
     * the consumer callback scope.
     */
    public void forEachBytes(java.util.function.Consumer<Bytes<?>> consumer) {
        ExcerptTailer tailer = queue.createTailer();
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                BytesStore<?, ?> store = dc.wire().read("fix").bytesStore();
                if (store == null)
                    break;
                consumer.accept(store.bytesForRead());
            }
        }
    }

    /**
     * Iterate over raw FIX bytes as BytesStore for zero-copy absolute reads.
     */
    public void forEachBytesStore(java.util.function.Consumer<BytesStore<?, ?>> consumer) {
        ExcerptTailer tailer = queue.createTailer();
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                BytesStore<?, ?> store = dc.wire().read("fix").bytesStore();
                if (store == null)
                    continue;
                consumer.accept(store);
            }
        }
    }

    private static String asciiFrom(BytesStore<?, ?> store) {
        long start = store.readPosition();
        long end = store.readLimit();
        int len = (int) (end - start);
        StringBuilder sb = new StringBuilder(len);
        for (long i = start; i < end; i++) {
            sb.append((char) store.readUnsignedByte(i));
        }
        return sb.toString();
    }

    @Override
    public void close() {
        queue.close();
    }
}


