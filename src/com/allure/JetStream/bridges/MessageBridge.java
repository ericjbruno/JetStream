package com.allure.JetStream.bridges;

import javax.jms.Queue;
import javax.jms.Topic;

/**
 * @author ericjbruno
 */
public interface MessageBridge {
    public void onNewJMSTopic(Topic topic);
    public void onNewJMSQueue(Queue queue);
}
