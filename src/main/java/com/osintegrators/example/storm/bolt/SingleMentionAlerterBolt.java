package com.osintegrators.example.storm.bolt;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SingleMentionAlerterBolt extends BaseBasicBolt
{
    private static final Logger logger = Logger.getLogger( SingleMentionAlerterBolt.class );
    private static final long serialVersionUID = 1L;

    private XMPPConnection xmppConnection;
    private String chatUsername = "your_gtalk_username";
    private String chatPassword = "your_gtalk_password";
    private String xmppHost;
    private String xmppPort;
    private String serviceName;
    
    Map<String, List<Date>> mentions = new HashMap<String, List<Date>>();
    
    
    public SingleMentionAlerterBolt( String xmppHost, String xmppPort, String serviceName )
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
        String mentionTarget = input.getStringByField( "mentionTarget" );
        Date mentionDate = (Date)input.getValueByField("date");
        
        logger.info( mentionTarget + " mentioned at: " + mentionDate );
        
        
        // send XMPP alert to user(s)
        try
        {

            Chat chat = ChatManager.getInstanceFor(xmppConnection).createChat( "some_user_of_interest@gmail.com", new MessageListener() {

                @Override
                public void processMessage(Chat chat, Message message)
                {
                    // TODO Auto-generated method stub
                
                }} );

            // google bounces back the default message types, you must use chat
            Message msgObj = new Message("some_user_of_interest@gmail.com", Message.Type.chat);
            msgObj.setBody(mentionTarget + " mentioned at: " + mentionDate );
            chat.sendMessage( msgObj );
        } 
        catch (SmackException e)
        {
            logger.error( "", e );
        }         
        
    }

    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }

    
}
