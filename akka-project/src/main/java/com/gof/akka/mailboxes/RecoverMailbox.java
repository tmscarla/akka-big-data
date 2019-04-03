package com.gof.akka.mailboxes;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedStablePriorityMailbox;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.typesafe.config.Config;

public class RecoverMailbox extends UnboundedStablePriorityMailbox {

    public RecoverMailbox(ActorSystem.Settings settings, Config config) {

        // Create a new PriorityGenerator, lower priority means more important
        super(
                new PriorityGenerator() {
                    @Override
                    public int gen(Object message) {
                        if (message instanceof Message) {
                            if(((Message) message).isRecovered()) {
                                return 0;
                            }
                            else return 1;
                        }
                        else if (message instanceof BatchMessage) {
                            if(((BatchMessage) message).isRecovered()) {
                                return 0;
                            }
                            else return 1;
                        }
                        else if (message.equals(PoisonPill.getInstance()))
                            return 2; // PoisonPill when no other left
                        else return 1; // Other messages
                    }
                });
    }
}
