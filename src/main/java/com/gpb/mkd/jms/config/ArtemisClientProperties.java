package com.gpb.mkd.jms.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.artemis")
public class ArtemisClientProperties {

    @NotBlank
    private String brokerUrl;

    @NotBlank
    private String username;

    @NotBlank
    private String password;

    @NotBlank
    private String requestQueue;

    @NotBlank
    private String replyQueue;

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
