import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * Created by sasikala on 11/10/15.
 */
public class Publisher220 {
    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String QUEUE_NAME_PREFIX = "queue.";
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
    InitialContext ctx;

    public Publisher220() throws JMSException, NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put(QUEUE_NAME_PREFIX + queueName, queueName);
        ctx = new InitialContext(properties);
        // Lookup connection factory
        QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
        queueConnection = connFactory.createQueueConnection();
        queueConnection.start();
        // Send message
        // create the message to send

        queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
    }
    public void sendMessages() throws NamingException, JMSException {

        Queue queue = (Queue)ctx.lookup(queueName);
        javax.jms.QueueSender queueSender = queueSession.createSender(queue);
        for (int i=0;i<10;i++) {
            TextMessage message = queueSession.createTextMessage("Test message: " + i);
            queueSender.send(message);
            System.out.println("Sent message: " + message.getText());
        }
        queueSender.close();
        queueSession.close();
        queueConnection.close();
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

    public static void main(String[] args) throws JMSException, NamingException {
        Publisher220 publisher220 = new Publisher220();
        publisher220.sendMessages();
    }
}
