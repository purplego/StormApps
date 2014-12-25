package twitter.stormImproved;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import twitter.tools.ArgsTools;
import twitter.tools.Args4Twitter;
import util.Constants;

public class HashtagsTopology {

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		TopologyBuilder builder = new TopologyBuilder();								// Construct a topology
		
		/*
		 * Topology definition: Define the spouts & bolts, and StreamGrouping used in this topology
		 */
		builder.setSpout("tweetReader", new TweetReaderSpout(), Args4Twitter.paraSpout);
		
		builder.setBolt("hashtagsSplitter", new HashtagsSplitterBolt(), Args4Twitter.paraBolt1)
				.shuffleGrouping("tweetReader", "tweet-stream");
		
		builder.setBolt("hashtagsCounter", new HashtagsCounterBolt(), Args4Twitter.paraBolt2)
				.fieldsGrouping("hashtagsSplitter", "tags-stream", new Fields("tag"));
		
		builder.setBolt("hashtagsMerger", new HashtagsMergerBolt(), Args4Twitter.paraBolt3)
				.shuffleGrouping("hashtagsCounter","tagCount-stream")
				.shuffleGrouping("tweetReader", "signal");
		
		/*
		 * Configuration
		 */
		Config conf = new Config();
//		conf.setDebug(true);
		conf.setDebug(false);
		
		/*
	     * How many executors to spawn for ackers.
	     * If this is set to 0, then Storm will immediately ack tuples as soon
	     * as they come off the spout, effectively disabling reliability.
	     */
		if(args!=null && args.length>0){
			conf.setNumWorkers(Args4Twitter.numWorkers);
			conf.setNumAckers(Args4Twitter.numAckers);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}else{
			conf.setMaxTaskParallelism(Args4Twitter.maxTaskPara);
			
			LocalCluster cluster = new LocalCluster();		//Define a LocalCluster object to simulate a in-process-cluster
			cluster.submitTopology("hashtags-count", conf, builder.createTopology());
			
//			Utils.sleep(10*60*60*1000);							// shutdown the local cluster in 100 minutes
//			
//			cluster.killTopology("hashtags-count");
//			cluster.shutdown();
		}
	}

}
