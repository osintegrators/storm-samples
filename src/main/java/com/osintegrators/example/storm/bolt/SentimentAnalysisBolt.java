package com.osintegrators.example.storm.bolt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jivesoftware.smack.Chat;
import org.jivesoftware.smack.ChatManager;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.MessageListener;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;

import twitter4j.Status;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SentimentAnalysisBolt extends BaseBasicBolt
{
    private static final Logger logger = Logger.getLogger( SentimentAnalysisBolt.class );
    private static final long serialVersionUID = 1L;

    private XMPPConnection xmppConnection;
    private String chatUsername = "your_gtalk_username";
    private String chatPassword = "your_gtalk_password";
    private String xmppHost;
    private String xmppPort;
    private String serviceName;
    private String afinnPath;
    
    Map<String, Integer> wordScores = new HashMap<String,Integer>();
    
    
    public SentimentAnalysisBolt( String xmppHost, String xmppPort, String serviceName, String afinnPath )
    {
        this.xmppHost = xmppHost;
        this.xmppPort = xmppPort;
        this.serviceName = serviceName;
        this.afinnPath = afinnPath;
    }
    
    
    
    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        super.prepare(stormConf, context);
        
        // load the AFINN data
        BufferedReader reader = null;
        try
        {
            FileReader fReader = new FileReader( this.afinnPath );
            reader = new BufferedReader( fReader );
            
            String line = null;
            while( (line = reader.readLine()) != null )
            {
                logger.warn( "LINE: " + line );
                
                String[] parts = line.split( "\t" );
                String word = parts[0];
                System.out.println( "parts[0] = " + parts[0]);
                Integer score = Integer.parseInt( parts[1] );
                
                wordScores.put( word.trim(), score );
                
            }
            
        }
        catch( Exception e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if( reader != null )
            {
                try
                {
                    reader.close();
                } 
                catch (IOException e)
                {}
            }
        }
        
        ConnectionConfiguration cc = new ConnectionConfiguration( xmppHost, Integer.parseInt( xmppPort ), serviceName );
        this.xmppConnection = new XMPPTCPConnection(cc);
        
        try
        {
            xmppConnection.connect();

            xmppConnection.login( chatUsername, chatPassword);
            
        }
        catch (SmackException e)
        {
            logger.error( "", e );
        } 
        catch (IOException e)
        {
            logger.error( "", e );
        } 
        catch (XMPPException e)
        {
            logger.error( "", e );
        }
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String mentionTarget = input.getStringByField( "mentionTarget" );
        Date mentionDate = (Date)input.getValueByField("date");
        Status status = (Status)input.getValueByField( "status" );
        
        logger.warn( "SENTIMENTANALYSIS: " + mentionTarget + " mentioned at: " + mentionDate );
        
        String text = status.getText();
        String[] words = text.split( " " );
        
        double sentimentScore = 0.0;
        for( String word : words )
        {
            System.out.println( "evaluating word: " + word );
            Integer tempScore = wordScores.get( word.trim());
            if( tempScore != null )
            {
                System.out.println( "sentiment value for word (" + word + ") is " + tempScore );
                sentimentScore += tempScore.intValue();
            }
            
        }
        
        sentimentScore = ( sentimentScore / words.length);
        System.out.println( "final sentiment score: " + sentimentScore );
        
        if( sentimentScore >= 0.5 || sentimentScore <= -0.5 )
        {
        
            try
            {
    
                Chat chat = ChatManager.getInstanceFor(xmppConnection).createChat( "motley.crue.fan@gmail.com", new MessageListener() {
    
                    @Override
                    public void processMessage(Chat chat, Message message)
                    {
                    
                    }} );
    
                // google bounces back the default message types, you must use chat
                Message msgObj = new Message("some_user_of_interest@gmail.com", Message.Type.chat);
                msgObj.setBody(mentionTarget + " mentioned at: " + mentionDate + ", with sentimentScore : " + sentimentScore + "( tweet id: " + status.getId() + ")" );
                chat.sendMessage( msgObj );
            } 
            catch (SmackException e)
            {
                logger.error( "", e );
            }         
        }
    }

    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }

    
}
