package com.fix.performance.flyweight;

/**
 * Minimal order view extracted from FIX without full object allocation.
 */
public final class Order {
    public String symbol;
    public int quantity;
    public long priceCents;

    public Order() {}

    public Order(String symbol, int quantity, long priceCents) {
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



