package com.gpb.mkd.jms.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.mkd.jms.dto.CompareActionType;
import com.gpb.mkd.jms.dto.ComparisonResponse;
import com.gpb.mkd.jms.dto.DifferenceType;
import com.gpb.mkd.jms.dto.JsonDifference;
import com.gpb.mkd.jms.repository.ComparisonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResultComparisonService {

    private final ComparisonRepository repository;
    private final JsonComparisonService jsonComparisonService;

    public ComparisonResponse compare(String eventId, CompareActionType actionType) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }

        if (actionType == null) {
            throw new IllegalArgumentException("actionType must be ANSWER or ANSWER_DETAIL");
        }

        String normalizedEventId = eventId.trim();

        log.info("[COMPARE][eventId={}][actionType={}] comparison START", normalizedEventId, actionType);

        JsonNode flinkResult = repository.findFlinkResult(normalizedEventId, actionType)
                .orElseThrow(() -> new IllegalStateException(
                        "Flink result not found for eventId=" + normalizedEventId + ", actionType=" + actionType
                ));

        log.info("[COMPARE][eventId={}][actionType={}] Flink result FOUND", normalizedEventId, actionType);

        JsonNode legacyResult = repository.findLegacyResultByEventId(normalizedEventId, actionType)
                .orElseThrow(() -> new IllegalStateException(
                        "Dotnet result not found for dfw_event_id=" + normalizedEventId + ", actionType=" + actionType
                ));

        log.info("[COMPARE][eventId={}][actionType={}] Dotnet result FOUND", normalizedEventId, actionType);

        String legacyQueryId = legacyResult.path("dfw_query_id").asText(null);

        JsonNode flinkComparable;
        JsonNode legacyComparable;

        if (actionType == CompareActionType.ANSWER_DETAIL) {
            flinkComparable = extractDetailResults(flinkResult, "FLINK");
            legacyComparable = extractDetailResults(legacyResult, "DOTNET");
        } else {
            legacyComparable = extractDotnetAnswerDetails(legacyResult);
            flinkComparable = extractFlinkAnswerByDotnetStructure(flinkResult, legacyComparable);
        }

        log.info(
                "[COMPARE][eventId={}][actionType={}] comparing flinkNodeType={} dotnetNodeType={} flinkSize={} dotnetSize={}",
                normalizedEventId, actionType,
                nodeType(flinkComparable), nodeType(legacyComparable),
                nodeSize(flinkComparable), nodeSize(legacyComparable)
        );

        List<JsonDifference> differences = jsonComparisonService.compare(flinkComparable, legacyComparable);

        int valueMismatch = count(differences, DifferenceType.VALUE_MISMATCH);
        int typeMismatch = count(differences, DifferenceType.TYPE_MISMATCH);
        int onlyInFlink = count(differences, DifferenceType.ONLY_IN_FLINK);
        int onlyInLegacy = count(differences, DifferenceType.ONLY_IN_LEGACY);

        boolean match = differences.isEmpty();

        ComparisonResponse response = new ComparisonResponse(
                normalizedEventId,
                actionType,
                match,
                legacyQueryId,
                valueMismatch,
                onlyInFlink,
                onlyInLegacy,
                typeMismatch,
                differences.size(),
                differences
        );

        logComparisonResult(response);

        log.info(
                "[COMPARE][eventId={}][actionType={}] comparison END match={} totalDifferences={}",
                normalizedEventId, actionType, match, differences.size()
        );

        return response;
    }

    private JsonNode extractDetailResults(JsonNode source, String sourceName) {
        if (source == null || source.isNull()) {
            throw new IllegalStateException(sourceName + " result is null");
        }

        JsonNode detailResults = source.get("detail_results");

        if (detailResults == null || detailResults.isNull() || !detailResults.isObject()) {
            throw new IllegalStateException(sourceName + " ANSWER_DETAIL does not contain detail_results object");
        }

        log.info("[COMPARE] {} ANSWER_DETAIL -> comparing detail_results", sourceName);

        return detailResults.deepCopy();
    }

    private JsonNode extractDotnetAnswerDetails(JsonNode source) {
        if (source == null || source.isNull()) {
            throw new IllegalStateException("DOTNET result is null");
        }

        JsonNode details = source.get("details");

        if (details == null || details.isNull() || !details.isObject()) {
            throw new IllegalStateException("DOTNET ANSWER does not contain details object");
        }

        log.info("[COMPARE] DOTNET ANSWER -> details selected as comparison structure");

        return details.deepCopy();
    }

    private JsonNode extractFlinkAnswerByDotnetStructure(JsonNode source, JsonNode dotnetDetails) {
        if (source == null || source.isNull()) {
            throw new IllegalStateException("FLINK result is null");
        }

        if (dotnetDetails == null || !dotnetDetails.isObject()) {
            throw new IllegalStateException("DOTNET details must be an object");
        }

        JsonNode data = source.get("data");

        if (data == null || data.isNull() || !data.isObject()) {
            throw new IllegalStateException("FLINK ANSWER does not contain data object");
        }

        JsonNode flinkDetails = data.get("details");

        if (flinkDetails == null || flinkDetails.isNull() || !flinkDetails.isObject()) {
            throw new IllegalStateException("FLINK ANSWER does not contain data.details object");
        }

        ObjectNode comparable = JsonNodeFactory.instance.objectNode();

        dotnetDetails.fieldNames().forEachRemaining(fieldName -> {
            JsonNode flinkValue = flinkDetails.get(fieldName);

            if (flinkValue != null) {
                comparable.set(fieldName, flinkValue.deepCopy());
            }
        });

        log.info(
                "[COMPARE] FLINK ANSWER -> selected {} blocks from data.details by DOTNET details structure",
                comparable.size()
        );

        return comparable;
    }

    private void logComparisonResult(ComparisonResponse response) {
        StringBuilder report = new StringBuilder();

        report.append("\n========== РЕЗУЛЬТАТ СРАВНЕНИЯ ==========\n");
        report.append("Event ID:                     ").append(response.eventId()).append('\n');
        report.append("Тип результата:               ").append(response.actionType()).append('\n');
        report.append("Результаты совпадают:         ").append(response.match() ? "ДА" : "НЕТ").append('\n');
        report.append("Dotnet Query ID:              ").append(response.legacyQueryId()).append("\n\n");

        report.append("---------- СТАТИСТИКА ----------\n");
        report.append("Несовпадений значений:        ").append(response.differentValues()).append('\n');
        report.append("Есть во Flink, нет в Dotnet:  ").append(response.onlyInFlink()).append('\n');
        report.append("Есть в Dotnet, нет во Flink:  ").append(response.onlyInLegacy()).append('\n');
        report.append("Несовпадений типов:           ").append(response.typeMismatches()).append('\n');
        report.append("Всего расхождений:            ").append(response.totalDifferences()).append("\n\n");

        report.append("---------- РАСХОЖДЕНИЯ ----------\n");

        if (response.differences().isEmpty()) {
            report.append("Расхождений нет.\n");
        } else {
            int number = 1;

            for (JsonDifference difference : response.differences()) {
                report.append("\nРасхождение №").append(number++).append('\n');
                report.append("Путь:    ").append(difference.path()).append('\n');
                report.append("Тип:     ").append(differenceTypeName(difference.type())).append('\n');
                report.append("Flink:   ").append(difference.flinkValue()).append('\n');
                report.append("Dotnet:  ").append(difference.legacyValue()).append('\n');
            }
        }

        report.append("\n==========================================");

        if (response.match()) {
            log.info("{}", report);
        } else {
            log.warn("{}", report);
        }
    }

    private String differenceTypeName(DifferenceType type) {
        if (type == null) {
            return "НЕИЗВЕСТНЫЙ ТИП";
        }

        return switch (type) {
            case VALUE_MISMATCH -> "НЕСОВПАДЕНИЕ ЗНАЧЕНИЙ";
            case TYPE_MISMATCH -> "НЕСОВПАДЕНИЕ ТИПОВ";
            case ONLY_IN_FLINK -> "ЕСТЬ ТОЛЬКО В FLINK";
            case ONLY_IN_LEGACY -> "ЕСТЬ ТОЛЬКО В DOTNET";
        };
    }

    private int count(List<JsonDifference> differences, DifferenceType type) {
        return (int) differences.stream()
                .filter(difference -> difference.type() == type)
                .count();
    }

    private String nodeType(JsonNode node) {
        return node == null ? "NULL" : node.getNodeType().name();
    }

    private int nodeSize(JsonNode node) {
        if (node == null) {
            return 0;
        }

        return node.isObject() || node.isArray() ? node.size() : 1;
    }
}