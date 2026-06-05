package com.gpb.mkd.jms;

import com.gpb.mkd.jms.config.ArtemisClientProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ArtemisClientProperties.class)
public class JmsPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(JmsPublisherApplication.class, args);
    }
}
