package com.gof.akka.messages;

import java.io.Serializable;
import java.util.List;


public class BatchMessage implements Serializable {

    private static final long serialVersionUID = 435813336929600776L;

    private final List<Message> messages;
    private final int size;

    public BatchMessage(final List<Message> messages) {
        super();
        this.messages = messages;
        this.size = messages.size();
    }

    public final List<Message> getMessages() {
        return messages;
    }

    public int getSize() {
        return size;
    }

    @Override
    public final String toString() {
        String repr = "[";
        for(Message m : messages) {
            repr = repr.concat("(" + m.getKey() + ", " + m.getVal() + ")");
        }
        repr = repr.concat("]");

        return repr;
    }

}
