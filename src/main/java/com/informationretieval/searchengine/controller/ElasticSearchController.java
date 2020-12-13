package com.informationretieval.searchengine.controller;

import com.informationretieval.searchengine.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller()
public class ElasticSearchController {

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
        model.addAttribute("hits", this.elasticSearchService.search(query, hashtags, mentions, synonyms, self, selected));
        model.addAttribute("users", this.elasticSearchService.getUsers(selected));
        model.addAttribute("topHashtags", this.elasticSearchService.getTopHashtags());
        model.addAttribute("topMentions", this.elasticSearchService.getTopMentions());
        model.addAttribute("query", query);
        model.addAttribute("hashtags", hashtags);
        model.addAttribute("mentions", mentions);
        model.addAttribute("synonyms", synonyms);
        model.addAttribute("self", self);
        return "search";
    }

    @GetMapping("/reset")
    public String reset() {
        this.elasticSearchService.reset();
        return "reset";
    }

    @GetMapping("/update")
    public String update() {
        this.elasticSearchService.update();
        return "update";
    }

    @GetMapping("/synonyms")
    public String synonyms() {
        this.elasticSearchService.synonyms();
        return "synonyms";
    }

}
