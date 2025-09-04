package com.fix.performance.flyweight;

/**
 * Minimal flyweight order view extracted from FIX without full object allocation.
 */
public final class FlyweightOrder {
    public String symbol;
    public int quantity;
    public long priceCents;

    public FlyweightOrder() {}

    public FlyweightOrder(String symbol, int quantity, long priceCents) {
        this.symbol = symbol;
        this.quantity = quantity;
        this.priceCents = priceCents;
    }

    public void set(String symbol, int quantity, long priceCents) {
        this.symbol = symbol;
        this.quantity = quantity;
        this.priceCents = priceCents;
    }
}


