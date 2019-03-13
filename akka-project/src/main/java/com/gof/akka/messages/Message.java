package com.gof.akka.messages;
import java.io.Serializable;

public class Message implements Serializable {

    private static final long serialVersionUID = 435813336929600776L;
    private final String key;
    private final String val;

    public Message(final String key, final String val) {
        super();
        this.key = key;
        this.val = val;
    }

    public final String getKey() {
        return key;
    }

    public final String getVal() {
        return val;
    }

    @Override
    public final String toString() {
        return "(" + key + ", " + val + ")";
    }

}
