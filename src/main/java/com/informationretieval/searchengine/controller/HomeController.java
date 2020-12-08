package com.informationretieval.searchengine.controller;

import com.informationretieval.searchengine.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import twitter4j.Logger;
import java.io.IOException;

@Controller()
public class HomeController {

    private static final Logger logger = Logger.getLogger(HomeController.class);
    private final IndexService indexService;

    @Autowired
    public HomeController(IndexService indexService) {
        this.indexService = indexService;
    }

    @RequestMapping("/")
    public String home(Model model, @RequestParam(required = false) String query, @RequestParam(required = false) String hashtags, @RequestParam(required = false) String mentions, @RequestParam(required = false) boolean synonyms) {
        try {
            logger.info("HOME-CONTROLLER: search query: " + query + " hashtags: " + hashtags + " mentions: " + mentions + " synonyms: " + synonyms);
            model.addAttribute("hits", this.indexService.search(query, hashtags, mentions, synonyms));
            model.addAttribute("topHashtags", this.indexService.getTopHashtags());
            model.addAttribute("topMentions", this.indexService.getTopMentions());
            model.addAttribute("query", query);
            model.addAttribute("hashtags", hashtags);
            model.addAttribute("mentions", mentions);
            model.addAttribute("synonyms", synonyms);
        } catch (IOException e) {
            logger.error("HOME-CONTROLLER: " + e.getMessage());
        }
        return "search";
    }



}
