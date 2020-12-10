package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;
import twitter4j.Status;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public interface TweetsIndexService {

    boolean create();
    boolean delete();
    boolean index(List<Status> statuses);
    List<String> getTopHashtags();
    List<String> getTopMentions();
    List<String> getKeywords(String id);

}
