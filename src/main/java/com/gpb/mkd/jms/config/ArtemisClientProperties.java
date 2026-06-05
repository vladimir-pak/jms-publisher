package com.gpb.mkd.jms.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.artemis")
public class ArtemisClientProperties {

    /**
     * Optional full broker URL override.
     * If it is not blank, it is used as-is and hosts/url settings below are ignored.
     */
    private String brokerUrl;

    /**
     * Multi-host Artemis connection endpoints. Used when brokerUrl is not set.
     */
    @Valid
    private List<Host> hosts = new ArrayList<>();

    /**
     * tcp or ssl. For the current requirements the default is ssl.
     */
    private String scheme = "ssl";

    /**
     * JMS clientId. Must be unique per simultaneously connected application instance.
     */
    @NotBlank
    private String clientId;

    /** Artemis HA mode. */
    private boolean ha = true;

    /** Number of initial connection attempts. */
    private int initialConnectAttempts = 3;

    /** -1 means reconnect forever. */
    private int reconnectAttempts = -1;

    /** Retry interval between reconnect attempts. */
    @Min(1)
    private long retryIntervalMs = 2_500;

    private boolean blockOnNonDurableSend = true;

    /** 0 disables consumer prefetch/window. Useful for strict request-response tests. */
    private int consumerWindowSize = 0;

    private boolean sslEnabled = true;

    /** Comma-separated TLS cipher suites or empty to use JVM/client defaults. */
    private String enabledCipherSuites;

    private String trustStorePath;
    private String trustStorePassword;
    private String trustStoreType = "PKCS12";

    private String keyStorePath;
    private String keyStorePassword;
    private String keyStoreType = "PKCS12";

    @NotBlank
    private String username;

    @NotBlank
    private String password;

    @NotBlank
    private String requestQueue;

    @NotBlank
    private String replyQueue;

    private Headers headers = new Headers();

    @Getter
    @Setter
    public static class Headers {

        /**
         * Header X_From.
         */
        private String xFrom;

        /**
         * Header X_ServiceID.
         */
        private String xServiceId;
    }

    /**
     * Max time to wait for a correlated response.
     */
    @Min(1000)
    private long receiveTimeoutMs = 30_000;

    /**
     * Size of the sender worker pool. Each worker thread lazily creates and reuses its own JMS Session and Producer.
     */
    @Min(1)
    private int concurrentSenders = 10;

    private boolean deliveryModePersistent = true;

    @Min(0)
    private long messageTtlMs = 60_000;

    /**
     * How long to keep replies that arrived before the sender registered the pending request.
     * This protects against the race caused by using the JMSMessageID/MQMD.MsgId request-response pattern.
     */
    @Min(1000)
    private long earlyReplyTtlMs = 60_000;

    @Min(1)
    private int earlyReplyMaxSize = 10_000;

    /**
     * Optional IBM MQ compatibility properties. These are useful when the JMS message is later bridged to IBM MQ
     * and the provider reads native MQMD fields.
     */
    private IbmMqCompatibility ibmMqCompatibility = new IbmMqCompatibility();

    @Getter
    @Setter
    public static class Host {
        @NotBlank
        private String host;

        @Min(1)
        private int port = 61617;
    }

    @Getter
    @Setter
    public static class IbmMqCompatibility {

        /**
         * Always set JMSReplyTo. This must stay true for request-response.
         */
        private boolean replyToEnabled = true;

        /**
         * Do not set request JMSCorrelationID by default.
         * Default pattern: provider returns response.CorrelId = request.MsgId.
         */
        private boolean requestCorrelationIdEnabled = false;

        private String requestCorrelationIdPrefix = "REQ-";

        /**
         * Optional IBM MQ JMS provider properties. Leave false for pure Artemis unless your bridge expects them.
         */
        private boolean ibmPropertiesEnabled = false;

        /** IBM MQ: MQMT_REQUEST = 1. */
        private int mqMessageType = 1;

        /** IBM MQ MQMD.Format value for string payloads. */
        private String mqFormat = "MQSTR";

        /** UTF-8 CCSID. */
        private int characterSet = 1208;

        /** IBM MQ common encoding value. */
        private int encoding = 546;
    }
}
