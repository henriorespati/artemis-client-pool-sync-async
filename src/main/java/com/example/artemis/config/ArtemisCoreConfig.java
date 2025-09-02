package com.example.artemis.config;

import com.example.artemis.core.pool.ConsumerPool;
import com.example.artemis.core.pool.ProducerPool;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
public class ArtemisCoreConfig {

    private final ArtemisProperties props;

    public ArtemisCoreConfig(ArtemisProperties props) {
        this.props = props;
    }

    // --- ServerLocator for SYNC sends (block until broker confirms)
    @Bean(name = "syncServerLocator")
    public ServerLocator syncServerLocator() throws Exception {
        return createServerLocator(true);
    }

    // --- ServerLocator for ASYNC sends (fire and forget)
    @Bean(name = "asyncServerLocator")
    public ServerLocator asyncServerLocator() throws Exception {
        return createServerLocator(false);
    }

    private ServerLocator createServerLocator(boolean sync) throws Exception {
        List<TransportConfiguration> transports = new ArrayList<>();
        for (String url : props.getBrokerUrls()) {
            String[] parts = url.split("://");
            String protocol = parts[0];
            String[] hp = parts[1].split(":");
            String host = hp[0];
            int port = Integer.parseInt(hp[1]);

            Map<String, Object> params = new HashMap<>();
            params.put("host", host);
            params.put("port", port);

            if ("ssl".equalsIgnoreCase(protocol)) {
                params.put("sslEnabled", true);
                if (!props.getTrustStorePath().isEmpty()) {
                    params.put("trustStorePath", props.getTrustStorePath());
                    params.put("trustStorePassword", props.getTrustStorePassword());
                }
                params.put("verifyHost", false);
            }

            transports.add(new TransportConfiguration(
                    NettyConnectorFactory.class.getName(), params));
        }

        ServerLocator locator = ActiveMQClient.createServerLocatorWithHA(
                transports.toArray(new TransportConfiguration[0])
        );

        // apply general connection properties
        locator.setRetryInterval(props.getRetryInterval());
        locator.setRetryIntervalMultiplier(props.getRetryIntervalMultiplier());
        locator.setMaxRetryInterval(props.getMaxRetryInterval());
        locator.setReconnectAttempts(props.getReconnectAttempts());
        locator.setUseTopologyForLoadBalancing(true);

        // configure sync/async behavior
        if (sync) {
            locator.setConfirmationWindowSize(-1);
            locator.setBlockOnDurableSend(true);
            locator.setBlockOnNonDurableSend(true);
        } else {
            locator.setConfirmationWindowSize(props.getConfirmationWindowSize());
            locator.setBlockOnDurableSend(false);
            locator.setBlockOnNonDurableSend(false);
        }

        return locator;
    }

    // --- ClientSessionFactories for each pool
    @Bean(name = "syncSessionFactory")
    public ClientSessionFactory syncSessionFactory(@Qualifier("syncServerLocator") ServerLocator syncServerLocator) throws Exception {
        return syncServerLocator.createSessionFactory();
    }

    @Bean(name = "asyncSessionFactory")
    public ClientSessionFactory asyncSessionFactory(@Qualifier("asyncServerLocator") ServerLocator asyncServerLocator) throws Exception {
        return asyncServerLocator.createSessionFactory();
    }

    // --- ProducerPools
    @Bean(name = "syncProducerPool", initMethod = "init", destroyMethod = "destroy")
    public ProducerPool syncProducerPool(@Qualifier("syncSessionFactory") ClientSessionFactory syncSessionFactory) {
        return new ProducerPool(syncSessionFactory, props);
    }

    @Bean(name = "asyncProducerPool", initMethod = "init", destroyMethod = "destroy")
    public ProducerPool asyncProducerPool(@Qualifier("asyncSessionFactory") ClientSessionFactory asyncSessionFactory) {
        return new ProducerPool(asyncSessionFactory, props);
    }

    @Bean
    public ConsumerPool consumerPool(
            @Qualifier("syncSessionFactory") ClientSessionFactory syncSessionFactory,
            @Qualifier("asyncSessionFactory") ClientSessionFactory asyncSessionFactory,
            ArtemisProperties props) {

        if ("SYNC".equalsIgnoreCase(props.getConsumerMode())) {
            return new ConsumerPool(syncSessionFactory, props);
        } else {
            return new ConsumerPool(asyncSessionFactory, props);
        }
    }

}
