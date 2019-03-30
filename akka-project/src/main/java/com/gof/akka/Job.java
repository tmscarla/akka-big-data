package com.gof.akka;

import com.gof.akka.messages.Message;
import com.gof.akka.operators.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Job {
    private List<Operator> operators;
    private String name;

    public Job(List<Operator> operators, String name) {
        this.operators = operators;
        this.name = name;
    }

    public List<Operator> getOperators() {
        return operators;
    }

    public String getName() {
        return name;
    }

    // JOB ONE
    public static final Job jobOne = new Job(Arrays.asList(
            new MapOperator("Map", 10,
                (String k, String v) -> new Message(k+Integer.toString(new Random().nextInt(100)), v+"pippo")),
            new SplitOperator("Split", 10),
            new MapOperator("MapParallel", 10,
                (String k, String v) -> new Message(k+Integer.toString(new Random().nextInt(100)), v.substring(0,5)+"words")),
            new FilterOperator("Filter", 10,
                (String k, String v) -> {if(k.hashCode() % 2 == 0)
                    { return true; } else { return false; }}),
            new MergeOperator("Merge", 10)), "jobOne");

    // JOB TWO
    public static final Job jobTwo = new Job(Arrays.asList(
            new FlatMapOperator("FlatMap", 5,
                    (String k, String v) -> {
                        List<Message> messages = new ArrayList<>();
                        for(int i=0; i<2; i++) {
                            String x = Integer.toString(new Random().nextInt(100));
                            messages.add(new Message(k+x, v+x));
                        }
                        return messages;
                    }),
            new SplitOperator("Split", 5),
            new AggregateOperator("Aggregate", 5,
                    (String k, List<String> vs) -> {
                        String res = "";
                        for(String v : vs) {
                            res = res.concat(v.substring(0, 3));
                        }
                        return new Message(k, res);
                    }, 10, 2),
            new FilterOperator("Filter", 5,
                    (String k, String v) -> {if(k.hashCode() % 2 == 1)
                    { return true; } else { return false; }}),
            new MergeOperator("Merge", 5),
            new MapOperator("Map", 5,
                    (String k, String v) -> new Message(k, v+".job2"))), "jobTwo");


}


