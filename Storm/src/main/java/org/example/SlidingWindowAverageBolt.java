package org.example;


import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class SlidingWindowAverageBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow window) {
        double sum = 0;
        int count = 0;

        for (Tuple tuple : window.get()) {
            sum += tuple.getInteger(0);
            count++;
        }

        if (count > 0) {
            double average = sum / count;
            System.out.println("Sliding Window Average: " + average);
            this.collector.emit(new Values(average));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sliding-average"));
    }
}
