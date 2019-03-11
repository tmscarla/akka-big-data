package com.gof.akka.messages;
import java.io.Serializable;

public class Message<K, V> implements Serializable {

    private static final long serialVersionUID = 435813336929600776L;
    private final K key;
    private final V val;

    public Message(final K key, final V val) {
        super();
        this.key = key;
        this.val = val;
    }

    public final K getKey() {
        return key;
    }

    public final V getVal() {
        return val;
    }

    @Override
    public final String toString() {
        return "(" + key.toString() + ", " + val.toString() + ")";
    }

}
