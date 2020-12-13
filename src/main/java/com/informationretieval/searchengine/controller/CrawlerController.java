package com.informationretieval.searchengine.controller;


import com.informationretieval.searchengine.service.CrawlerService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller()
@RequestMapping("/crawler")
public class CrawlerController {

    final CrawlerService crawlerService;

    public CrawlerController(CrawlerService crawlerService) {
        this.crawlerService = crawlerService;
    }

    @GetMapping("/start")
    public String start(@RequestParam String user) {
        this.crawlerService.start(user);
        return "crawler";
    }



}
