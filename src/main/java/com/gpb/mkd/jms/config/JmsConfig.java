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
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                properties.getBrokerUrl(),
                properties.getUsername(),
                properties.getPassword()
        );

        factory.setCallTimeout(properties.getReceiveTimeoutMs());
        factory.setCallFailoverTimeout(properties.getReceiveTimeoutMs());

        return factory;
    }
}
