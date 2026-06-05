package com.gpb.mkd.jms.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MessageResult {

    private int index;

    private boolean success;

    private String requestMessageId;

    private String responseCorrelationId;

    private String responseBody;

    private long durationMs;

    private String error;
}
