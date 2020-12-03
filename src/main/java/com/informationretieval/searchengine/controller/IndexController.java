package com.informationretieval.searchengine.controller;

import com.informationretieval.searchengine.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import twitter4j.Logger;

import java.io.IOException;

@Controller()
@RequestMapping("/index")
public class IndexController {

    private static final Logger logger = Logger.getLogger(IndexController.class);
    private final IndexService indexService;

    @Autowired
    public IndexController(IndexService indexService) {
        this.indexService = indexService;
    }

    @GetMapping("/create")
    public String create() {
        try {
            logger.info("INDEX-CONTROLLER: create index");
            boolean acknowledged = this.indexService.create();
            if (acknowledged) {
                return "create";
            }
        } catch (IOException e) {
            logger.error("INDEX-CONTROLLER: " + e.getMessage());
        }
        return "error";
    }

    @GetMapping("/delete")
    public String delete() {
        try {
            logger.info("INDEX-CONTROLLER: delete index");
            boolean acknowledged = this.indexService.delete();
            if (acknowledged) {
                return "delete";
            }
        } catch (IOException e) {
            logger.error("INDEX-CONTROLLER: " + e.getMessage());
        }
        return "error";
    }

}
