package topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolts.CassandraBolt;
import bolts.UnshortenBolt;
import spouts.TwitterSpout;

/**
 * Orchestrates the elements and forms a topology to run the unshortening service.
 * 
 * @author Michael Vogiatzis
 */
public class UnshortenTopology {
	
	public static void main (String[] args) throws Exception{
	TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout("spout", new TwitterSpout(), 1);
    
    builder.setBolt("unshortenBolt", new UnshortenBolt(), 4)
             .shuffleGrouping("spout");
    builder.setBolt("dbBolt", new CassandraBolt(), 2)
             .shuffleGrouping("unshortenBolt");

    Config conf = new Config();
    conf.setDebug(false);

    //submit it to the cluster, or submit it locally
    if(args!=null && args.length > 0) {
        conf.setNumWorkers(3);
        
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {        
        conf.setMaxTaskParallelism(10);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("unshortening", conf, builder.createTopology());

        while(true) {
            if (!Thread.interrupted())
                Thread.sleep(500);
        }

        //cluster.shutdown();
    }
	}
}
