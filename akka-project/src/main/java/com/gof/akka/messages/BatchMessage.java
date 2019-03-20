package com.gof.akka.messages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// BatchMessage, a message which contains a list of different Message objects
public class BatchMessage implements Serializable {
    private List<Message> messages = new ArrayList<>();

    public BatchMessage(List<Message> messages) {
        super();
        this.messages.addAll(messages);
    }

    public final List<Message> getMessages() {
        return messages;
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
