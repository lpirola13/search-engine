package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;
import twitter4j.Status;
import twitter4j.User;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public interface ElasticSearchService {

    void reset();
    void update();
    List<Map<String, Object>> search(String query, String hashtags, String mentions, boolean synonyms, boolean self, String id, Date fromDate, Date toDate);
    List<Map<String,String>> getUsers(String selected);
    List<String>  getTopHashtags();
    List<String>  getTopMentions();

    void synonyms();

    boolean indexUser(User user);

    boolean indexTweets(List<Status> statuses);
}
