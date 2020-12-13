package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;
import twitter4j.Status;

import java.util.List;

@Service
public interface TweetsIndexService {

    boolean create();
    boolean delete();
    boolean synonyms(List<String> synonyms);
    boolean index(List<Status> statuses);
    List<String> getTopHashtags();
    List<String> getTopMentions();
    List<String> getKeywords(String id);
    List<String> getHastags(String id);
}
