package logquery.improvedstorm;

import util.Constants;
import logquery.tools.ArgsTools;
import logquery.tools.ArgsLogquery;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class LogQueryTopology {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws  Exception {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		TopologyBuilder builder = new TopologyBuilder();	// Construct a topology
		
		/*
		 * Topology definition: Define the spouts & bolts, and StreamGrouping used in this topology
		 */
		builder.setSpout("logReader", new LogGeneratorSpout(), ArgsLogquery.paraSpout);
		
		builder.setBolt("logAnalyzer", new LogAnalyzerBolt(), ArgsLogquery.paraBolt1)
				.shuffleGrouping("logReader", "log-stream");
		
		builder.setBolt("merger", new MergerBolt(), ArgsLogquery.paraBolt2)
				.shuffleGrouping("logAnalyzer","pv-stream")
				.shuffleGrouping("logAnalyzer","uv-stream")
				.shuffleGrouping("logAnalyzer","status-stream")
				.shuffleGrouping("logAnalyzer","spider-stream");
		
		/*builder.setBolt("printer", new PrinterBolt(), ArgsLogquery.paraBolt3)
			   .shuffleGrouping("merger", "global-pv-stream")
			   .shuffleGrouping("merger", "global-uv-stream")
			   .shuffleGrouping("merger", "global-status-stream")
			   .shuffleGrouping("merger", "global-spider-stream");*/
				
		/*
		 * Configuration
		 */
		Config conf = new Config();
		conf.setDebug(true);
//		conf.setDebug(false);
		
		/*
	     * How many executors to spawn for ackers.
	     * If this is set to 0, then Storm will immediately ack tuples as soon
	     * as they come off the spout, effectively disabling reliability.
	     */
		if(args!=null && args.length>0){
			conf.setNumWorkers(ArgsLogquery.numWorkers);
			conf.setNumAckers(ArgsLogquery.numAckers);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}else{
			conf.setMaxTaskParallelism(ArgsLogquery.maxTaskPara);
			
			LocalCluster cluster = new LocalCluster();		//Define a LocalCluster object to simulate a in-process-cluster
			cluster.submitTopology("log-query", conf, builder.createTopology());
			
			Thread.sleep(100*60*1000);							// shutdown the local cluster in 100 minutes
			
			cluster.killTopology("log-query");
			cluster.shutdown();
		}
	}

}
