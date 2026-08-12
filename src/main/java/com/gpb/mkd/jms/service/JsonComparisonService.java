package com.gpb.mkd.jms.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.gpb.mkd.jms.config.ComparisonProperties;
import com.gpb.mkd.jms.dto.DifferenceType;
import com.gpb.mkd.jms.dto.JsonDifference;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class JsonComparisonService {

    private final ComparisonProperties properties;

    public List<JsonDifference> compare(
            JsonNode flink,
            JsonNode legacy
    ) {
        List<JsonDifference> result =
                new ArrayList<>();

        compareNode(
                "$",
                flink,
                legacy,
                result
        );

        return List.copyOf(result);
    }

    private void compareNode(
            String path,
            JsonNode flink,
            JsonNode legacy,
            List<JsonDifference> out
    ) {
        if (out.size() >= properties.getMaxDifferences()) {
            return;
        }

        if (flink == null || flink.isMissingNode()) {
            if (legacy != null && !legacy.isMissingNode()) {
                out.add(
                        diff(
                                path,
                                DifferenceType.ONLY_IN_LEGACY,
                                null,
                                legacy
                        )
                );
            }

            return;
        }

        if (legacy == null || legacy.isMissingNode()) {
            out.add(
                    diff(
                            path,
                            DifferenceType.ONLY_IN_FLINK,
                            flink,
                            null
                    )
            );

            return;
        }

        if (flink.isObject() && legacy.isObject()) {
            compareObjects(
                    path,
                    flink,
                    legacy,
                    out
            );

            return;
        }

        if (flink.isArray() && legacy.isArray()) {
            compareArrays(
                    path,
                    flink,
                    legacy,
                    out
            );

            return;
        }

        if (flink.getNodeType() != legacy.getNodeType()) {
            out.add(
                    diff(
                            path,
                            DifferenceType.TYPE_MISMATCH,
                            flink,
                            legacy
                    )
            );

            return;
        }

        if (!flink.equals(legacy)) {
            out.add(
                    diff(
                            path,
                            DifferenceType.VALUE_MISMATCH,
                            flink,
                            legacy
                    )
            );
        }
    }

    private void compareObjects(
            String path,
            JsonNode flink,
            JsonNode legacy,
            List<JsonDifference> out
    ) {
        Set<String> names =
                new LinkedHashSet<>();

        flink.fieldNames()
                .forEachRemaining(names::add);

        legacy.fieldNames()
                .forEachRemaining(names::add);

        for (String name : names) {
            if (out.size() >= properties.getMaxDifferences()) {
                return;
            }

            JsonNode flinkValue =
                    flink.get(name);

            JsonNode legacyValue =
                    legacy.get(name);

            String childPath =
                    path + "." + name;

            if (flinkValue == null) {
                out.add(
                        diff(
                                childPath,
                                DifferenceType.ONLY_IN_LEGACY,
                                null,
                                legacyValue
                        )
                );

            } else if (legacyValue == null) {
                out.add(
                        diff(
                                childPath,
                                DifferenceType.ONLY_IN_FLINK,
                                flinkValue,
                                null
                        )
                );

            } else {
                compareNode(
                        childPath,
                        flinkValue,
                        legacyValue,
                        out
                );
            }
        }
    }

    /**
     * Массивы сравниваем без учёта исходного порядка элементов.
     *
     * Например:
     *
     * flink:
     * ["A", "B", "C"]
     *
     * legacy:
     * ["C", "A", "B"]
     *
     * будут считаться одинаковыми.
     */
    private void compareArrays(
            String path,
            JsonNode flink,
            JsonNode legacy,
            List<JsonDifference> out
    ) {
        List<JsonNode> flinkItems =
                toSortedList(flink);

        List<JsonNode> legacyItems =
                toSortedList(legacy);

        int max =
                Math.max(
                        flinkItems.size(),
                        legacyItems.size()
                );

        for (int i = 0; i < max; i++) {
            if (out.size() >= properties.getMaxDifferences()) {
                return;
            }

            JsonNode flinkValue =
                    i < flinkItems.size()
                            ? flinkItems.get(i)
                            : null;

            JsonNode legacyValue =
                    i < legacyItems.size()
                            ? legacyItems.get(i)
                            : null;

            compareNode(
                    path + "[" + i + "]",
                    flinkValue,
                    legacyValue,
                    out
            );
        }
    }

    /**
     * Преобразуем JSON array в List и сортируем.
     *
     * Сортировка делается по полной JSON-строке элемента.
     *
     * Работает как для:
     *
     * ["A", "B"]
     *
     * так и для:
     *
     * [
     *   {"id": 1, "status": "SUCCESS"},
     *   {"id": 2, "status": "ERROR"}
     * ]
     */
    private List<JsonNode> toSortedList(
            JsonNode array
    ) {
        List<JsonNode> result =
                new ArrayList<>();

        array.forEach(result::add);

        result.sort(
                Comparator.comparing(
                        this::canonicalValue
                )
        );

        return result;
    }

    /**
     * Значение для сортировки.
     *
     * Для простых значений:
     * "ABC", 123, true
     *
     * Для объектов/массивов:
     * их JSON representation.
     */
    private String canonicalValue(
            JsonNode node
    ) {
        if (node == null) {
            return "";
        }

        if (node.isTextual()) {
            return "TEXT:" + node.asText();
        }

        if (node.isNumber()) {
            return "NUMBER:" + node.toString();
        }

        if (node.isBoolean()) {
            return "BOOLEAN:" + node.asBoolean();
        }

        if (node.isNull()) {
            return "NULL:";
        }

        return node.toString();
    }

    private JsonDifference diff(
            String path,
            DifferenceType type,
            JsonNode flink,
            JsonNode legacy
    ) {
        return new JsonDifference(
                path,
                type,
                render(flink),
                render(legacy)
        );
    }

    private String render(
            JsonNode node
    ) {
        if (node == null) {
            return null;
        }

        String value =
                node.isTextual()
                        ? node.asText()
                        : node.toString();

        int max =
                Math.max(
                        50,
                        properties.getMaxValueLength()
                );

        return value.length() <= max
                ? value
                : value.substring(0, max) + "...";
    }
}