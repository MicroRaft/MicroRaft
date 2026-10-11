package io.microraft.impl.statemachine;

public final class NoOp {

    /** A shared operation that does not change state. */
    public static final NoOp INSTANCE = new NoOp();

    private NoOp() {
    }

}
