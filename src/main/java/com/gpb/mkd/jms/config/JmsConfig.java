package com.gpb.mkd.jms.config;

import lombok.RequiredArgsConstructor;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class JmsConfig {

    private final ArtemisClientProperties properties;

    @Bean
    public ActiveMQConnectionFactory artemisConnectionFactory() {
        String brokerUrl = ArtemisBrokerUrlBuilder.build(properties);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                brokerUrl,
                properties.getUsername(),
                properties.getPassword()
        );

        factory.setClientID(properties.getClientId());
        factory.setCallTimeout(properties.getReceiveTimeoutMs());
        factory.setCallFailoverTimeout(properties.getReceiveTimeoutMs());

        factory.setInitialConnectAttempts(properties.getInitialConnectAttempts());
        factory.setReconnectAttempts(properties.getReconnectAttempts());
        factory.setRetryInterval(properties.getRetryIntervalMs());
        factory.setBlockOnNonDurableSend(properties.isBlockOnNonDurableSend());
        factory.setConsumerWindowSize(properties.getConsumerWindowSize());

        return factory;
    }
}
