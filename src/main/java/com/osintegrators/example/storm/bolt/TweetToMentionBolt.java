package com.osintegrators.example.storm.bolt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import twitter4j.Status;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetToMentionBolt extends BaseBasicBolt
{
    private static final Logger logger = Logger.getLogger( TweetToMentionBolt.class );
    private static final long serialVersionUID = 1L;

    
    private List<String> mentionTargets = new ArrayList<String>();
    
    public TweetToMentionBolt( final List<String> srcMentionTargets )
    {
        this.mentionTargets.addAll( srcMentionTargets );
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {

    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        Status status = (Status)input.getValueByField("tweet");
        
        // parse the status and look for mentions of the entity we're interested in...
        String statusText = status.getText();
        
        logger.info( "status: " + statusText );
        
        for( String mentionTarget : mentionTargets )
        {
            if( statusText.toLowerCase().matches( ".*\\s*" + mentionTarget + "\\s+.*" ))
            {
                logger.info( "emitting metion: " + mentionTarget );
                collector.emit( new Values( mentionTarget, new Date(), status ) );
            }
            else
            {
                // NOP
            }
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("mentionTarget", "date", "status" ));
    }

    
}
