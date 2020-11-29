package com.informationretieval.searchengine.controller;

import com.informationretieval.searchengine.service.IndexService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import twitter4j.Logger;

import java.io.IOException;

@RestController()
@RequestMapping("/index")
public class IndexController {

    private static final Logger logger = Logger.getLogger(IndexController.class);
    final IndexService indexService;


    public IndexController(IndexService indexService) {
        this.indexService = indexService;
    }

    @GetMapping("/create")
    public String createIndex() {
        try {
            boolean acknowledged = this.indexService.createIndex();
            if (acknowledged) {
                return "Index Created!";
            }
        } catch (IOException e) {
            logger.error("INDEX-CONTROLLER: " + e.getMessage());
        }
        return "Something went wrong!";
    }

    @GetMapping("/delete")
    public String deleteIndex() {
        try {
            boolean acknowledged = this.indexService.deleteIndex();
            if (acknowledged) {
                return "Index Deleted!";
            }
        } catch (IOException e) {
            logger.error("INDEX-CONTROLLER: " + e.getMessage());
        }
        return "Something went wrong!";
    }


}
