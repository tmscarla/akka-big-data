package com.gof.akka.messages.create;

import java.io.Serializable;

import akka.actor.Address;
import com.gof.akka.functions.MapFunction;


public class CreateMapMsg extends CreateMsg {
    private static final long serialVersionUID = 3169449768254646491L;
    private MapFunction fun;

    public CreateMapMsg(String name, String color, int posStage, boolean isLocal,
                        Address address, int batchSize, final MapFunction fun) {
        super(name, color, posStage, isLocal, address, batchSize);
        this.fun = fun;
    }

    public MapFunction getFun() {
        return fun;
    }

    @Override
    public String toString() {
        return ""; //TODO
    }

}

