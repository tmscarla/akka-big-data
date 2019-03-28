package com.gof.akka.messages.source;

import java.io.Serializable;

public class RandSourceMsg  implements Serializable {
    private final int keySize;
    private final int valueSize;

    public RandSourceMsg(int keySize, int valueSize) {
        this.keySize = keySize;
        this.valueSize = valueSize;
    }

    public int getKeySize() {
        return keySize;
    }

    public int getValueSize() {
        return valueSize;
    }
}
