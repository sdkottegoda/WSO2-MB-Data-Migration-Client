import javax.jms.JMSException;
import javax.naming.NamingException;

/**
 * Created by sasikala on 11/10/15.
 */
public class Main {
    public static void main(String[] args) throws NamingException, JMSException {
        Thread subscriber = new Thread(new Subscriber());
        subscriber.start();
    }
}
