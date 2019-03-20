package com.gof.akka.messages;
import java.io.Serializable;


// A message exchanged between functions. It consists in a (key, value) pair of strings.
public class Message implements Serializable {
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
