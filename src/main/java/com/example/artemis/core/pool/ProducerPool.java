package com.example.artemis.core.pool;

import com.example.artemis.config.ArtemisProperties;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Semaphore;

public class ProducerPool {

    private static final Logger logger = LoggerFactory.getLogger(ProducerPool.class);

    private final ClientSessionFactory sessionFactory;
    private final ArtemisProperties props;

    private final Deque<ClientSession> pool = new ArrayDeque<>();
    private final Semaphore semaphore;

    public ProducerPool(ClientSessionFactory sessionFactory, ArtemisProperties props) {
        this.sessionFactory = sessionFactory;
        this.props = props;
        this.semaphore = new Semaphore(props.getProducerPoolSize());
    }

    public void init() throws ActiveMQException {
        for (int i = 0; i < props.getProducerPoolSize(); i++) {
            logger.info("Initializing session {}", i);
            logger.debug("Connected via connector: {}", sessionFactory.getConnectorConfiguration().getCombinedParams());
            ClientSession session = sessionFactory.createSession(
                    props.getUser(),
                    props.getPassword(),
                    true,
                    true,
                    true,
                    true,
                    1
            );
            session.start();
            pool.push(session);
        }
        logger.info("Initialized ProducerPool with {} sessions", props.getProducerPoolSize());
    }

    private ClientSession acquire() throws InterruptedException {
        semaphore.acquire();
        synchronized (pool) {
            ClientSession session = pool.poll();
            if (session == null) {
                logger.info("No available session, creating one on demand");
                return createOnDemandSession();
            }
            return session;
        }
    }

    private void release(ClientSession session) {
        synchronized (pool) {
            pool.push(session);
        }
        semaphore.release();
    }

    private ClientSession createOnDemandSession() {
        try {
            ClientSession session = sessionFactory.createSession(
                    props.getUser(),
                    props.getPassword(),
                    true,
                    true,
                    true,
                    true,
                    1
            );
            session.start();
            return session;
        } catch (ActiveMQException e) {
            throw new RuntimeException("Failed to create on-demand session", e);
        }
    }

    public void destroy() {
        synchronized (pool) {
            for (ClientSession session : pool) {
                try {
                    session.close();
                } catch (Exception e) {
                    logger.warn("Error closing session", e);
                }
            }
            pool.clear();
        }
        logger.info("ProducerPool closed");
    }

    // Sync send (ephemeral producer)
    public void sendSync(String queueName, String message) throws Exception {
        ClientSession session = acquire();
        try {
            ClientProducer producer = session.createProducer(queueName);
            ClientMessage clientMessage = session.createMessage(true);
            clientMessage.getBodyBuffer().writeString(message);
            clientMessage.putStringProperty("JMS_QUEUE", queueName);
            producer.send(clientMessage);
            producer.close(); // ephemeral
        } finally {
            release(session);
        }
    }

    // Async send (ephemeral producer + handler)
    public void sendAsync(String queueName, String message, SendAcknowledgementHandler handler) throws Exception {
        ClientSession session = acquire();
        try {
            ClientProducer producer = session.createProducer(queueName);
            ClientMessage clientMessage = session.createMessage(true);
            clientMessage.getBodyBuffer().writeString(message);
            clientMessage.putStringProperty("JMS_QUEUE", queueName);
            producer.send(clientMessage, handler);
            producer.close(); // ephemeral
        } finally {
            release(session);
        }
    }
}
