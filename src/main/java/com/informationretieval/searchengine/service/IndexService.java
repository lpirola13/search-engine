package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;
import twitter4j.Status;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public interface IndexService {

    boolean create() throws IOException;
    boolean delete() throws IOException;
    boolean index(List<Status> statuses) throws IOException;
    List<Map<String, Object>> search(String query, String hashtags, String mentions) throws IOException;
    List<String> getTopHashtags() throws IOException;
    List<String> getTopMentions() throws IOException;

}
