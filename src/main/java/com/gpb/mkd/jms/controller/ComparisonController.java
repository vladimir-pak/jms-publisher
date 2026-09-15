package com.gpb.mkd.jms.controller;

import com.gpb.mkd.jms.dto.CompareActionType;
import com.gpb.mkd.jms.dto.ComparisonResponse;
import com.gpb.mkd.jms.service.ResultComparisonService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/compare")
@RequiredArgsConstructor
public class ComparisonController {

    private final ResultComparisonService service;

    /**
     * Examples:
     * GET /api/compare/070100...?actionType=ANSWER
     * GET /api/compare/070100...?actionType=ANSWER_DETAIL
     */
    @GetMapping("/{eventId}")
    public ComparisonResponse compare(
            @PathVariable String eventId,
            @RequestParam CompareActionType actionType
    ) {
        return service.compare(eventId, actionType);
    }
}
