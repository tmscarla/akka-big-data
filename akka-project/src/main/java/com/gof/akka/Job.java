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

    public static final Job jobOne = new Job(Arrays.asList(
            new MapOperator("Map", 10,
                (String k, String v) -> new Message(k+Integer.toString(new Random().nextInt(100)), v+"pippo")),
            new SplitOperator("Split", 10),
            new MapOperator("MapParallel", 10,
                (String k, String v) -> new Message(k+Integer.toString(new Random().nextInt(100)), v+"try")),
            new FilterOperator("Filter", 10,
                (String k, String v) -> {if(k.hashCode() % 2 == 0)
                    { return true; } else { return false; }}),
            new MergeOperator("Merge", 10)), "jobOne");

}


