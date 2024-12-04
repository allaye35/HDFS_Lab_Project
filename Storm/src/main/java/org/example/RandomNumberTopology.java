package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
public class RandomNumberTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("random-spout", new RandomNumberSpout(),4)
                .setNumTasks(4);

        builder.setBolt("odd-filter", new OddNumberFilterBolt(), 1)
                .shuffleGrouping("random-spout");

        builder.setBolt("average", new AveragingBolt(), 1)
                .shuffleGrouping("odd-filter");

        builder.setBolt("sliding-average",
                        new SlidingWindowAverageBolt()
                                .withWindow(Count.of(10), Count.of(2))
                        , 1)
                .shuffleGrouping("odd-filter");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        if (args != null && args.length > 0) {
            try {
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("random-numbers", conf, builder.createTopology());
            Utils.sleep(30000);
            cluster.shutdown();
        }
    }
}