package com.fix.performance.fix;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Utility to build minimal FIX 4.4 messages (NewOrderSingle, OrderCancelRequest) with proper
 * BodyLength (9) and CheckSum (10) calculation. Session sequence number is intentionally omitted
 * per project constraints.
 */
public final class FixMessageBuilder {
    private static final char SOH = '\u0001';

    private static final DateTimeFormatter SENDING_TIME_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final String senderCompId;
    private final String targetCompId;

    public FixMessageBuilder(String senderCompId, String targetCompId) {
        this.senderCompId = senderCompId;
        this.targetCompId = targetCompId;
    }

    public String buildNewOrderSingle(String clOrdId, String symbol, char side, int quantity,
            long priceCents, long nanoTimestamp) {
        String sendingTime = SENDING_TIME_FMT.format(Instant.now());

        StringBuilder body = new StringBuilder(128);
        body.append("35=D").append(SOH).append("49=").append(senderCompId).append(SOH).append("56=")
                .append(targetCompId).append(SOH).append("52=").append(sendingTime).append(SOH)
                .append("11=").append(clOrdId).append(SOH).append("55=").append(symbol).append(SOH)
                .append("54=").append(side).append(SOH).append("38=").append(quantity).append(SOH)
                .append("40=2").append(SOH) // Limit
                .append("44=").append(formatPrice(priceCents)).append(SOH).append("60=")
                .append(nanoTimestamp).append(SOH);

        return finalizeMessage(body);
    }

    public String buildOrderCancelRequest(String clOrdId, String origClOrdId, String symbol,
            char side, long nanoTimestamp) {
        String sendingTime = SENDING_TIME_FMT.format(Instant.now());

        StringBuilder body = new StringBuilder(96);
        body.append("35=F").append(SOH).append("49=").append(senderCompId).append(SOH).append("56=")
                .append(targetCompId).append(SOH).append("52=").append(sendingTime).append(SOH)
                .append("11=").append(clOrdId).append(SOH).append("41=").append(origClOrdId)
                .append(SOH).append("55=").append(symbol).append(SOH).append("54=").append(side)
                .append(SOH).append("60=").append(nanoTimestamp).append(SOH);

        return finalizeMessage(body);
    }

    private static String formatPrice(long cents) {
        long abs = Math.abs(cents);
        long dollars = abs / 100L;
        long remainder = abs % 100L;
        String s = dollars + "." + (remainder < 10 ? "0" : "") + remainder;
        return cents < 0 ? "-" + s : s;
    }

    private static String finalizeMessage(StringBuilder body) {
        byte[] bodyBytes = body.toString().getBytes(StandardCharsets.US_ASCII);
        String header = "8=FIX.4.4" + SOH + "9=" + bodyBytes.length + SOH;
        String withoutChecksum = header + body;
        int checksum = checksum(withoutChecksum.getBytes(StandardCharsets.US_ASCII));
        String checksumField = String.format("10=%03d" + SOH, checksum);
        return withoutChecksum + checksumField;
    }

    /**
     * FIX checksum is the modulo 256 of the sum of all bytes before tag 10.
     */
    public static int checksum(byte[] bytes) {
        int sum = 0;
        for (byte b : bytes) {
            sum += (b & 0xFF);
        }
        return sum % 256;
    }
}


