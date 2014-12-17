package com.osintegrators.example.storm.main;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

import com.osintegrators.example.storm.bolt.MentionsThresholdBolt;
import com.osintegrators.example.storm.bolt.SentimentAnalysisBolt;
import com.osintegrators.example.storm.bolt.SingleMentionAlerterBolt;
import com.osintegrators.example.storm.bolt.TweetToMentionBolt;
import com.osintegrators.example.storm.spout.TwitterSpout;
import com.osintegrators.example.storm.util.StormRunner;


public class TwitterAnalyticsTopology 
{
	private static final Logger logger = Logger.getLogger(TwitterAnalyticsTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 800000000;
	
	private String topologyName = "";
	private final TopologyBuilder builder;
	private final Config topologyConfig;
	private final int runtimeInSeconds;
	
	public TwitterAnalyticsTopology()
	{
	    logger.info( "building TwitterAnalyticsTopology...");
	    this.topologyName = "TwitterAnalyticsTopology";
        this.builder = new TopologyBuilder();
        this.topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();	    
	}

	public TwitterAnalyticsTopology( final String topologyName )
	{
	    logger.info( "building TwitterAnalyticsTopology...");
	    this.topologyName = topologyName;
        this.builder = new TopologyBuilder();
        this.topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();
	}
	
	private void wireTopology()
	{
	    logger.info( "Wiring topology...");
	    
        String consumerKey = "your_consumer_key";
        String consumerSecret = "your_consumer_secret";
        String accessToken = "your_access_token";
        String accessTokenSecret = "your_token_secret";
        String[] keywords = {};
        
        List<String> mentionTargets = new ArrayList<String>();
        
        mentionTargets.add( "facebook" );
        mentionTargets.add( "tor" );
        mentionTargets.add( "oracle" );
        mentionTargets.add( "jive" );
        mentionTargets.add( "manufacturing" );
        mentionTargets.add( "openstack" );
        mentionTargets.add( "barrett" );
        mentionTargets.add( "hadoop" );
        mentionTargets.add( "arduino" );
        mentionTargets.add( "memristor" );
        mentionTargets.add( "sony" );
        mentionTargets.add( "scala" );
        
        this.builder.setSpout(  "twitterSpout", new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keywords ), 1); 	  
        this.builder.setBolt(   "tweetToMentionBolt", new TweetToMentionBolt(mentionTargets), 3 ).shuffleGrouping("twitterSpout");
        this.builder.setBolt(   "singleMentionAlerterBolt", new SingleMentionAlerterBolt( "talk.google.com", "5222", "gmail.com" ), 1).shuffleGrouping("tweetToMentionBolt");
        this.builder.setBolt(   "mentionsThresholdBolt", new MentionsThresholdBolt("talk.google.com", "5222", "gmail.com"), 1).shuffleGrouping("tweetToMentionBolt");
        this.builder.setBolt(   "sentimentAnalysisBolt",  new SentimentAnalysisBolt( "talk.google.com", "5222", "gmail.com", "/home/prhodes/workspaces/storm/storm-samples/AFINN/AFINN-111.txt" ), 3).shuffleGrouping( "tweetToMentionBolt" );
	}
	
	private Config createTopologyConfiguration()
	{
	    Config conf = new Config();
	    conf.setDebug(false);
	    return conf;	    
	}
	
	public static void main(String[] args) throws Exception 
	{
	    
	    String topologyName = "";
	    
	    if (args.length >= 1) 
	    {
	        topologyName = args[0];
	    }
	      
	    boolean runLocally = true;
	    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) 
	    {
	        runLocally = false;
	    }
	    
	    TwitterAnalyticsTopology ttt = null;
	    if( !topologyName.isEmpty() )
	    {
	        ttt = new TwitterAnalyticsTopology(topologyName);
	    }
	    else
	    {
	        ttt = new TwitterAnalyticsTopology();
	    }
	    
	    logger.info( "Topology Name: " + ttt.topologyName );   
	    
	    
	    if (runLocally) 
	    {
	        logger.info("Running in local mode");
	        ttt.runLocally();
	    }
	    else 
	    {
	        logger.info("Running in remote (cluster) mode");
	        ttt.runRemotely();
	    }	    
	}
	
	private void runLocally() throws Exception
	{
	    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);    
	}
	
	private void runRemotely() throws Exception
	{      	    
	    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	}
	
}
