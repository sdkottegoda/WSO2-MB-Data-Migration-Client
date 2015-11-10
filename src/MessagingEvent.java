import com.lmax.disruptor.EventFactory;

import javax.jms.TextMessage;

/**
 * Created by sasikala on 11/9/15.
 */
public class MessagingEvent {

    TextMessage message;

    public void setMessage(TextMessage aTextMessage){
        message = aTextMessage;
    }
    public static class PublishEventFactory implements EventFactory<MessagingEvent> {

        @Override
        public MessagingEvent newInstance() {
            return new MessagingEvent();
        }
    }



    public static EventFactory<MessagingEvent> getFactory() {
        return new PublishEventFactory();
    }
}
