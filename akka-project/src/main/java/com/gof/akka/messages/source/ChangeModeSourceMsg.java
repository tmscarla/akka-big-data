package com.gof.akka.messages.source;

import java.io.Serializable;

public class ChangeModeSourceMsg implements Serializable {
    private boolean batchMode;

    public ChangeModeSourceMsg(boolean batchMode) {
        this.batchMode = batchMode;
    }

    public boolean isBatchMode() {
        return batchMode;
    }
}
