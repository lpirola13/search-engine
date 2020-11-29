package com.informationretieval.searchengine.controller;


import com.informationretieval.searchengine.service.CrawlerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController()
@RequestMapping("/crawler")
public class CrawlerController {

    final CrawlerService crawlerService;

    public CrawlerController(CrawlerService crawlerService) {
        this.crawlerService = crawlerService;
    }

    @GetMapping("/start")
    public String startCrawler() {
        this.crawlerService.startCrawler();
        return "Crawler started!";
    }

    @GetMapping("/search")
    public String searchCrawler() {
        this.crawlerService.searchDoc();
        return "Crawler started!";
    }

    @GetMapping("/bonsai")
    public String tryBonsai() {
        this.crawlerService.testBonsai();
        return "Bonsai tested!";
    }

    @GetMapping("/indexing")
    public String indexing() {
        this.crawlerService.crawlProfile();
        return "crawl tested!";
    }

}
