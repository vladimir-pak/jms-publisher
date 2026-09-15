package com.gpb.mkd.jms.dto;

import java.util.List;

public record ComparisonResponse(
        String eventId,
        CompareActionType actionType,
        boolean match,
        String legacyQueryId,
        int differentValues,
        int onlyInFlink,
        int onlyInLegacy,
        int typeMismatches,
        int totalDifferences,
        List<JsonDifference> differences
) {
}
