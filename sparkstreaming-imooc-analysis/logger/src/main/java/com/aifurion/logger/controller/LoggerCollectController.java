package com.aifurion.logger.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ：zzy
 * @description：TODO 日志收集
 * @date ：2021/12/4 18:10
 */


@RestController
public class LoggerCollectController {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerCollectController.class);

    @PostMapping("/savelog")
    public String saveLog(@RequestBody String logString) {

        LOGGER.info(logString);

        return "success";

    }

}
