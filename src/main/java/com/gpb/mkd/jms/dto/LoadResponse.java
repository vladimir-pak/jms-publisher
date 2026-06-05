package com.gpb.mkd.jms.dto;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class LoadResponse {

    private int requested;
    private int success;
    private int failed;

    private long totalDurationMs;

    private double requestsPerSecond;
    private double avgLatencyMs;

    private long minLatencyMs;
    private long maxLatencyMs;

    private List<MessageResult> results;
}
