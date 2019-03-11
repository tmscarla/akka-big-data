package com.gof.akka.messages;

import java.io.Serializable;

public class Simple implements Serializable{
    private final String key;

    public Simple(String key) {
        super();
        this.key = key;
    }

    public final int getNum() {
        return 3;
    }

}
