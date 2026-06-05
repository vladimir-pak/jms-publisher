package com.gpb.mkd.jms.controller;

import com.gpb.mkd.jms.dto.LoadRequest;
import com.gpb.mkd.jms.dto.LoadResponse;
import com.gpb.mkd.jms.service.JmsRequestReplyService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/load")
@RequiredArgsConstructor
public class LoadTestController {

    private final JmsRequestReplyService service;

    @PostMapping("/request-reply")
    public LoadResponse requestReply(@Valid @RequestBody LoadRequest request) {
        return service.runLoadTest(request);
    }
}
