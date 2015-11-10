import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.wso2.andes.thread.DefaultThreadFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by sasikala on 11/9/15.
 */
public class Subscriber extends Thread{

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    String queueName = "testQueue";
    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private MessageConsumer consumer;

    private Disruptor<MessagingEvent> disruptor;
    ExecutorService executorPool;
    private static final int EXECUTOR_POOL_SHUTDOWN_WAIT_TIME = 10;

    public Subscriber() throws JMSException, NamingException {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("DisruptorMessagingEventThread-%d").build();
        executorPool = Executors.newCachedThreadPool(namedThreadFactory);

        int bufferSize = 4096;
        disruptor = new Disruptor<>(MessagingEvent.getFactory(), bufferSize, executorPool, ProducerType.SINGLE,
                                    new BlockingWaitStrategy());

        disruptor.handleEventsWith(new Publisher());
        disruptor.start();

        registerSubscriber();
    }

    public void shutDown() throws JMSException {
        // Housekeeping
        consumer.close();
        queueSession.close();
        queueConnection.stop();
        queueConnection.close();
    }

    public void registerSubscriber() throws NamingException, JMSException

    {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue."+ queueName,queueName);
        InitialContext ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        queueSession =
                queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        //Receive message
        Queue queue =  (Queue) ctx.lookup(queueName);
        consumer = queueSession.createConsumer(queue);
    }
    public void receiveMessages() throws NamingException, JMSException {

        TextMessage message = (TextMessage) consumer.receive();
        System.out.println("Got message from queue receiver==>" + message.getText());
        publishToDisrutptor(message);
    }

    public void publishToDisrutptor(TextMessage message){
        RingBuffer<MessagingEvent> ringBuffer = disruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        MessagingEvent evt = ringBuffer.get(sequence);
        evt.setMessage(message);
        ringBuffer.publish(sequence);
        System.out.println("Published message to ring buffer");

    }
    public void run(){
        while (true){
            try {
                receiveMessages();

            } catch (NamingException e) {
                e.printStackTrace();
                break;
            } catch (JMSException e) {
                e.printStackTrace();
                break;
            }
        }
    }
    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }
}
