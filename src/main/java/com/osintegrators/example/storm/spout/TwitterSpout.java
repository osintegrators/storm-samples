package com.osintegrators.example.storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import twitter4j.DirectMessage;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterSpout extends BaseRichSpout
{
    private static final Logger logger = Logger.getLogger( TwitterSpout.class );
    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream twitterStream;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String[] keyWords;
    
    
    public TwitterSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keyWords) 
    {
        logger.info( "TwitterSpout ctor called...");
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keyWords = keyWords;
    }    
    
    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        logger.info( "TwitterSpout opening...");
        this.collector = collector;
        this.queue = new LinkedBlockingQueue<Status>(1000);
        
        
        UserStreamListener listener = new UserStreamListener() 
        {

            @Override
            public void onStatus(Status status) 
            {
                logger.info( "onStatus() called");
                logger.info( "Status: " + status.getUser().getName() + " : "  + status.getText() );
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) 
            {
            }

            @Override
            public void onTrackLimitationNotice(int i) 
            {
            }

            @Override
            public void onScrubGeo(long l, long l1) 
            {
            }

            @Override
            public void onException(Exception ex) 
            {
                logger.error( "Exception: \n", ex);
            }

            @Override
            public void onStallWarning(StallWarning arg0) 
            {
            }

            @Override
            public void onBlock(User arg0, User arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onDeletionNotice(long arg0, long arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onDirectMessage(DirectMessage arg0)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onFavorite(User arg0, User arg1, Status arg2)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onFollow(User arg0, User arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onFriendList(long[] arg0)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUnblock(User arg0, User arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUnfavorite(User arg0, User arg1, Status arg2)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListCreation(User arg0, UserList arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListDeletion(User arg0, UserList arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListMemberAddition(User arg0, User arg1,
                    UserList arg2)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListMemberDeletion(User arg0, User arg1,
                    UserList arg2)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListSubscription(User arg0, User arg1,
                    UserList arg2)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListUnsubscription(User arg0, User arg1,
                    UserList arg2)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserListUpdate(User arg0, UserList arg1)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void onUserProfileUpdate(User arg0)
            {
                // TODO Auto-generated method stub
                
            }   
            
        };        
        
        
        this.twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        this.twitterStream.addListener(listener);
        this.twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        this.twitterStream.setOAuthAccessToken(token);
        
        if (keyWords.length == 0) 
        {
            logger.info( "Sample Twitter Stream");
            this.twitterStream.user();
        }

        else {

            FilterQuery query = new FilterQuery().track(keyWords);
            this.twitterStream.filter(query);
        }        
    }
    
    
    @Override
    public void close() 
    {
        this.twitterStream.shutdown();
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet"));
        
    }
    
    public void nextTuple()
    {
        // logger.info( "Called nextTuple()");
        Status ret = queue.poll();
        if (ret == null) 
        {
            // logger.info( "No value to emit...");
            Utils.sleep(50);
        } 
        else 
        {
            // logger.info( "Emitting value...");
            collector.emit(new Values(ret));
        }
    }
    
}