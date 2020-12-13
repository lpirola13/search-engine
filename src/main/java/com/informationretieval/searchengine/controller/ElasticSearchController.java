package com.informationretieval.searchengine.controller;

import com.informationretieval.searchengine.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import twitter4j.Logger;

@Controller()
public class ElasticSearchController {

    private static final Logger logger = Logger.getLogger(ElasticSearchController.class);
    private final ElasticSearchService elasticSearchService;

    @Autowired
    public ElasticSearchController(ElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }

    @RequestMapping("/")
    public String home(Model model,
                       @RequestParam(required = false) String query,
                       @RequestParam(required = false) String hashtags,
                       @RequestParam(required = false) String mentions,
                       @RequestParam(required = false) boolean synonyms,
                       @RequestParam(required = false) boolean self,
                       @RequestParam(required = false) String selected) {
        if (selected != null) {
            logger.info("HOME-CONTROLLER: search user: " + selected + " query: " + query + " hashtags: " + hashtags + " mentions: " + mentions + " synonyms: " + synonyms + " self: " + self);
        } else {
            logger.info("HOME-CONTROLLER: search query: " + query + " hashtags: " + hashtags + " mentions: " + mentions + " synonyms: " + synonyms + " self: " + self);
        }
        model.addAttribute("hits", this.elasticSearchService.search(query, hashtags, mentions, synonyms, self, selected));
        model.addAttribute("users", this.elasticSearchService.makeUsers(selected));
        model.addAttribute("topHashtags", this.elasticSearchService.getTopHashtags());
        model.addAttribute("topMentions", this.elasticSearchService.getTopMentions());
        model.addAttribute("query", query);
        model.addAttribute("hashtags", hashtags);
        model.addAttribute("mentions", mentions);
        model.addAttribute("synonyms", synonyms);
        model.addAttribute("self", self);
        return "search";
    }

}
