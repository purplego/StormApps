/**
 * @author Purple Wang
 * @2014-02-25
 */
package wordcount.storm;

import util.Constants;
import wordcount.tools.ArgsWordcount;
import wordcount.tools.ArgsTools;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {
	
		public static void main(String[] args) throws Exception{
		
			ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);
			
			// TODO Auto-generated method stub
			TopologyBuilder builder = new TopologyBuilder();		// parse args and reset the Constants class
			
			builder.setSpout("spout", new RandomSentenceSpout(), ArgsWordcount.paraSpout);
			
			builder.setBolt("split-bolt", new SplitSentenceBolt(), ArgsWordcount.paraBolt1)
				   .shuffleGrouping("spout", "sentence-stream");
			
			builder.setBolt("count-bolt", new WordCountBolt(), ArgsWordcount.paraBolt2)
				   .fieldsGrouping("split-bolt", "word-stream", new Fields("word"));
			
			Config conf = new Config();
//			conf.setDebug(true);
			conf.setDebug(false);
			
			if(args!=null && args.length>0){
				conf.setNumWorkers(ArgsWordcount.numWorkers);
				conf.setNumAckers(ArgsWordcount.numAckers);
				
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			}else{
				conf.setMaxTaskParallelism(3);
				
				//Define a LocalCluster object to simulate a in-process-cluster
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("word-count", conf, builder.createTopology());
				
				Thread.sleep(10*60*1000);
				
				cluster.killTopology("word-count");
				cluster.shutdown();
			}
	}

}
