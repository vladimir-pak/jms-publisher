package com.gpb.mkd.jms.config;

import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds an Artemis broker URL from application.yaml settings.
 *
 * Example:
 * (ssl://host1:61617,ssl://host2:61617)?ha=true;initialConnectAttempts=3;reconnectAttempts=-1;retryInterval=2500;...
 */
public final class ArtemisBrokerUrlBuilder {

    private ArtemisBrokerUrlBuilder() {
    }

    public static String build(ArtemisClientProperties properties) {
        if (StringUtils.hasText(properties.getBrokerUrl())) {
            return properties.getBrokerUrl();
        }

        if (properties.getHosts() == null || properties.getHosts().isEmpty()) {
            throw new IllegalArgumentException("Either app.artemis.broker-url or app.artemis.hosts must be configured");
        }

        String endpoints = properties.getHosts().stream()
                .map(host -> properties.getScheme() + "://" + host.getHost() + ":" + host.getPort())
                .collect(Collectors.joining(","));

        List<String> params = new ArrayList<>();
        params.add("ha=" + properties.isHa());
        params.add("clientID=" + properties.getClientId());
        params.add("initialConnectAttempts=" + properties.getInitialConnectAttempts());
        params.add("reconnectAttempts=" + properties.getReconnectAttempts());
        params.add("retryInterval=" + properties.getRetryIntervalMs());
        params.add("blockOnNonDurableSend=" + properties.isBlockOnNonDurableSend());
        params.add("consumerWindowSize=" + properties.getConsumerWindowSize());
        params.add("sslEnabled=" + properties.isSslEnabled());

        addIfHasText(params, "enabledCipherSuites", properties.getEnabledCipherSuites());
        addIfHasText(params, "trustStorePath", properties.getTrustStorePath());
        addIfHasText(params, "trustStorePassword", properties.getTrustStorePassword());
        addIfHasText(params, "trustStoreType", properties.getTrustStoreType());
        addIfHasText(params, "keyStorePath", properties.getKeyStorePath());
        addIfHasText(params, "keyStorePassword", properties.getKeyStorePassword());
        addIfHasText(params, "keyStoreType", properties.getKeyStoreType());

        return "(" + endpoints + ")?" + String.join(";", params);
    }

    private static void addIfHasText(List<String> params, String key, String value) {
        if (StringUtils.hasText(value)) {
            params.add(key + "=" + value);
        }
    }
}
