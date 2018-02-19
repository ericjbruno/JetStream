package com.allure.JetStream.bridges;

import static com.allure.JetStream.JetStream.startTime;
import com.allure.JetStream.JetStreamMessage;
import java.util.HashMap;
import java.util.Hashtable;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.MqttCallback;

/**
 * @author ericjbruno
 * 
 * This class bridges between MQTT and JMS Topics
 * 
 */
public class MQTTBridge implements /*MessageBridge,*/ MqttCallback, MessageListener {
    int qos             = 2;
    String broker       = "tcp://localhost:1883";
    String clientId     = "bridge_JSQMQTT";
    
    MemoryPersistence persistence = new MemoryPersistence();
    MqttClient mqttClient = null;
    boolean connected = false;
    
    public static final String JETSTREAM =
            "com.allure.JetStream.jndi.JetStreamInitialContextFactory";

    public static String topicName = "";
    private static Context jndi = null;

    private Connection connection = null;
    private Session session;
    private MessageConsumer consumer = null;
    private String uuid = null;

    class JMSTopic {
        Topic topic = null;
        MessageProducer producer = null;
        MessageConsumer consumer = null;
    }
    HashMap<String, JMSTopic> jmsTopics = new HashMap<>();

    public MQTTBridge() {
//        connectMQTT();
//        connectJMS();
        //JetStream.getInstance().registerBridge(this);
    }
    
    private boolean connectMQTT() {
        try {
            mqttClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            
            System.out.println("Connecting to broker: "+broker);
            mqttClient.connect(connOpts);

            System.out.println("Connected");
            connected = true;
            
            // Subscribe to everything
            mqttClient.setCallback(this);
            mqttClient.subscribe("#"); // Everything wildcard
            return true;
        } 
        catch ( MqttException me ) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
        
        return false;
    }
    
    private boolean connectJMS() {
        try {
            // Create a JMS connection
            //
            Hashtable env = new Hashtable();
            env.put("java.naming.factory.initial", 
                    "com.allure.JetStream.jndi.JetStreamInitialContextFactory");
            jndi = new InitialContext(env);

            ConnectionFactory conFactory =
                (ConnectionFactory)jndi.lookup("ConnectionFactory");

            connection = conFactory.createConnection("","");
            connection.start();
            
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic all = session.createTopic( "***" );
            MessageConsumer consumer = session.createConsumer(all);
            consumer.setMessageListener(this);
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
        return false;
    }
    
    public void shutdown() {
        if ( connected && mqttClient != null ) {
                try {
                    mqttClient.disconnect();
                    System.out.println("Disconnected");
                    connected = false;
                }
            catch ( MqttException me ) {
                System.out.println("reason "+me.getReasonCode());
                System.out.println("msg "+me.getMessage());
                System.out.println("loc "+me.getLocalizedMessage());
                System.out.println("cause "+me.getCause());
                System.out.println("excep "+me);
                me.printStackTrace();
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////
    // com.allure.JetStream.bridges
    //////////////////////////////////////////////////////////////////////////

//    @Override
    public void onNewJMSTopic(Topic topic) {
        
    }

//    @Override
    public void onNewJMSQueue(Queue queue) {
        
    }

    //////////////////////////////////////////////////////////////////////////
    // org.eclipse.paho.client.mqttv3.MqttCallback
    //////////////////////////////////////////////////////////////////////////
    HashMap<String, String> pubTopics = new HashMap<>();

    @Override
    public void deliveryComplete(IMqttDeliveryToken imdt) {    
    }

    @Override
    public void connectionLost(Throwable thrwbl) {
        System.out.println("Disconnected");
    }

    @Override
    public void messageArrived(String topicName, MqttMessage msg) throws Exception {
        //System.out.println( topicName+": " + msg.toString() );
        
        // Ignore this message if the source was this bridge
        //if ( pubTopic.equals(topicName) ) {
        if ( pubTopics.containsKey(topicName) ) {
            return;
        }
            
        try {
            JMSTopic jmsTopic = jmsTopics.get(topicName);
            if ( jmsTopic == null || jmsTopic.topic == null) {
                // First time for a message on this MQTT topic
                // Create all the JMS plumbing needed
                jmsTopic = new JMSTopic();
                try {
                    jmsTopic.topic = (Topic)jndi.lookup( topicName );
                }
                catch ( Exception e ) { }
                if ( jmsTopic.topic == null )
                    jmsTopic.topic = session.createTopic( topicName );

                jmsTopic.producer = session.createProducer(jmsTopic.topic);
                jmsTopic.consumer = session.createConsumer(jmsTopic.topic);
                jmsTopic.consumer.setMessageListener(this);

                jmsTopics.put(topicName, jmsTopic);
            }
        
            // Create and send a JMS BytesMessage on the Topic
            BytesMessage jmsmsg = session.createBytesMessage();
            jmsmsg.writeBytes( msg.getPayload() );
            jmsmsg.setLongProperty("guid", startTime);
            jmsTopic.producer.send(jmsmsg);
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    
    public static boolean ArraysAreEquals(  byte[] first,
                                            int firstOffset,
                                            int firstLength,
                                            byte[] second,
                                            int secondOffset,
                                            int secondLength) {
        if( firstLength != secondLength ) {
            return false;
        }

        for( int index = 0; index < firstLength; ++index ) {
            if( first[firstOffset+index] != second[secondOffset+index]) {
                return false;
            }
        }

        return true;
    }

    //////////////////////////////////////////////////////////////////////////
    // javax.jms.MessageListener
    //////////////////////////////////////////////////////////////////////////

    int msgCount = 0;
    @Override
    public void onMessage(Message msg) {
//        long start = System.currentTimeMillis();
//        try {
//            // Ignore this message if the source was this bridge
//            if ( msg.propertyExists("guid") ) {
//                Long guid = msg.getLongProperty("guid");
//                if ( guid != null && guid.longValue() == startTime ) {
//                    return;
//                }
//            }
//            
//            BytesMessage bytesMsg = (BytesMessage)msg;
//            JetStreamMessage jsMsg = (JetStreamMessage)msg;
//            String name = jsMsg.getDestinationName();
//            byte[] bytes = new byte[(int)bytesMsg.getBodyLength()];
//            bytesMsg.readBytes(bytes);
//            MqttMessage message = new MqttMessage( bytes );
//            message.setQos(qos);
//
//            synchronized ( pubTopics ) {
//                pubTopics.put(name, name); 
//            }
//            
//            mqttClient.publish(name, message);
//        }
//        catch ( Exception e ) {
//            e.printStackTrace();
//        }
//        
//        synchronized ( pubTopics ) {
//            pubTopics.remove(topicName);
//        }
//        long end = System.currentTimeMillis();
//        msgCount++;
//        if ( msgCount > 1000 ) {
//            msgCount = 0;
//            System.out.println("onMessage TIME = " + (end-start) );
//        }

    }
}
