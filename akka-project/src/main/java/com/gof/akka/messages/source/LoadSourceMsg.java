package com.gof.akka.messages.source;

import java.io.Serializable;

public class LoadSourceMsg implements Serializable {
    private final String filePath;

    public LoadSourceMsg(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }
}
