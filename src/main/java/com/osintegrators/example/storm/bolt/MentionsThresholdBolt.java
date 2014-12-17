package com.osintegrators.example.storm.bolt;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MentionsThresholdBolt extends BaseBasicBolt
{
    private static final Logger logger = Logger.getLogger( MentionsThresholdBolt.class );
    private static final long serialVersionUID = 1L;

    private XMPPConnection xmppConnection;
    private String chatUsername = "your_gtalk_username";
    private String chatPassword = "your_gtalk_password";
    private String xmppHost;
    private String xmppPort;
    private String serviceName;
    
    Map<String, SortedSet<Date>> mentions = new HashMap<String, SortedSet<Date>>();
    
    
    public MentionsThresholdBolt( String xmppHost, String xmppPort, String serviceName )
    {
        this.xmppHost = xmppHost;
        this.xmppPort = xmppPort;
        this.serviceName = serviceName;
    }
    
    
    
    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        super.prepare(stormConf, context);
        
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
        // store mentions and calculate a running metric of "mentions / hour"
        // if the mph exceeds our threshold, send an alert
        // we could also log it to a database as a point in time snapshot for historical reporting
        
        String mentionTarget = input.getStringByField( "mentionTarget" );
        Date mentionDate = (Date)input.getValueByField("date");
        logger.info( "MENTIONSTHRESHOLDBOLT: " + mentionTarget + " mentioned at: " + mentionDate );
        
        // get the current running mention list for this mentionTarget,
        // prune anything older than our threshold (one hour for this example)
        // add the current mention, and sum the mentions in the past hour
        // if we're over our threshold, send an alert via XMPP
        SortedSet<Date> mentionsForTarget = mentions.get( mentionTarget );
        if( mentionsForTarget == null )
        {
            mentionsForTarget = new TreeSet<Date>();
            mentionsForTarget.add(mentionDate);
            mentions.put(mentionTarget, mentionsForTarget);
        }
        else
        {
            // we found an existing list, add our new entry and remove anything older than an hour
            mentionsForTarget.add(mentionDate);
            
            
            Iterator<Date> iter = mentionsForTarget.iterator();
            
            // setup a Date instance for one hour ago
            Calendar now = GregorianCalendar.getInstance();
            now.add(Calendar.HOUR, -1 );
            
            
            while( iter.hasNext() )
            {
                Date d = iter.next();
                if( d.before(now.getTime()))
                {
                    iter.remove();
                }
                else
                {
                    // we've found a record that is within the hour window, we can stop looking at the others, since this set is sorted
                    break;
                }
            }
            
            int mentionsInPastHour = mentionsForTarget.size();
            if( mentionsInPastHour > 2 )
            {
                // send XMPP alert to user(s)
                try
                {

                    Chat chat = ChatManager.getInstanceFor(xmppConnection).createChat( "motley.crue.fan@gmail.com", new MessageListener() {

                        @Override
                        public void processMessage(Chat chat, Message message)
                        {
                            // TODO Auto-generated method stub
                        
                        }} );

                    // google bounces back the default message types, you must use chat
                    Message msgObj = new Message("some_user_of_interest@gmail.com", Message.Type.chat);
                    msgObj.setBody(mentionTarget + " mentioned " + mentionsInPastHour + " times in past hour. " + new Date().toString() );
                    chat.sendMessage( msgObj );
                } 
                catch (SmackException e)
                {
                    logger.error( "", e );
                }       
            }
            
        }        
    }

    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }

    
}
