package com.gof.akka.messages.create;

import akka.actor.Address;
import com.gof.akka.functions.FlatMapFunction;

public class CreateFlatMapMsg extends CreateMsg {
        private FlatMapFunction fun;

        public CreateFlatMapMsg(String name, String color, int posStage, boolean isLocal,
                                Address address, int batchSize, final FlatMapFunction fun) {
            super(name, color, posStage, isLocal, address, batchSize);
            this.fun = fun;
        }

        public FlatMapFunction getFun() {
            return fun;
        }

        @Override
        public String toString() {
            return ""; //TODO
        }

}
