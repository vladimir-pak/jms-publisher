package com.gpb.mkd.jms.dto;

public record JsonDifference(
        String path,
        DifferenceType type,
        String flinkValue,
        String legacyValue
) {
}
