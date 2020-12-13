package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public interface ElasticSearchService {

    boolean resetIndices();
    boolean updateUsersProfile();
    List<Map<String, Object>> search(String query, String hashtags, String mentions, boolean synonyms, boolean self, String id);
    List<Map<String,String>> makeUsers(String selected);
    List<String>  getTopHashtags();
    List<String>  getTopMentions();
}
