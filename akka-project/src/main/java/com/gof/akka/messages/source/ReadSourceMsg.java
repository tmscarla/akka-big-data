package com.gof.akka.messages.source;

import java.io.Serializable;

public class ReadSourceMsg implements Serializable {
    private final String filePath;

    public ReadSourceMsg(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }
}
