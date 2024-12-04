package org.example;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.LinkedList;
import java.util.Queue;

public class AveragingBolt extends BaseBasicBolt {
    private Queue<Integer> numbers;
    private static final int WINDOW_SIZE = 10;

    public AveragingBolt() {
        this.numbers = new LinkedList<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        int number = input.getInteger(0);
        numbers.add(number);

        if (numbers.size() > WINDOW_SIZE) {
            numbers.poll();
        }

        if (numbers.size() == WINDOW_SIZE) {
            double average = numbers.stream()
                    .mapToInt(Integer::intValue)
                    .average()
                    .orElse(0.0);

            System.out.println("Manual Window Average: " + average);
            collector.emit(new Values(average));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("average"));
    }
}