package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.ElasticSearchService;
import com.informationretieval.searchengine.service.TweetsIndexService;
import com.informationretieval.searchengine.service.UsersIndexService;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import twitter4j.Logger;
import java.io.IOException;
import java.util.*;

@Service
public class ElasticSearchServiceImpl implements ElasticSearchService {

    private static final Logger logger = Logger.getLogger(ElasticSearchServiceImpl.class);
    private final RestHighLevelClient restHighLevelClient;
    private final TweetsIndexService tweetsIndexService;
    private final UsersIndexService usersIndexService;

    @Autowired
    public ElasticSearchServiceImpl(RestHighLevelClient restHighLevelClient, TweetsIndexService tweetsIndexService, UsersIndexService usersIndexService) {
        this.restHighLevelClient = restHighLevelClient;
        this.tweetsIndexService = tweetsIndexService;
        this.usersIndexService = usersIndexService;
    }

    @Override
    @SuppressWarnings("deprecation")
    public boolean resetIndices() {

        logger.info("ELASTIC-SEARCH-SERVICE: reset indices");

        boolean delete = true;
        GetIndexRequest request = new GetIndexRequest();
        request.indices("tweets");
        try {
            if (restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT)) {
                delete = this.tweetsIndexService.delete();
                Thread.sleep(10000);
            }
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage());
            delete = false;
        }
        if (delete) {
            if (this.tweetsIndexService.create()) {
                request = new GetIndexRequest();
                request.indices("users");
                try {
                    if (restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT)) {
                        delete = this.usersIndexService.delete();
                        Thread.sleep(10000);
                    }
                } catch (IOException | InterruptedException e) {
                    logger.info(e.getMessage());
                    delete = false;
                }
                if (delete) {
                    if (this.usersIndexService.create()) {
                        return true;
                    } else {
                        logger.error("ELASTIC-SEARCH-SERVICE: error during users create");
                        return false;
                    }
                } else {
                    logger.error("ELASTIC-SEARCH-SERVICE: error during users delete");
                    return false;
                }
            } else {
                logger.error("ELASTIC-SEARCH-SERVICE: error during tweets create");
                return false;
            }
        } else {
            logger.error("ELASTIC-SEARCH-SERVICE: error during tweets delete");
            return false;
        }
    }

    @Override
    public boolean updateUsersProfile() {

        List<Map<String, Object>> users = this.usersIndexService.getUsers();
        for (Map<String, Object> user : users) {
            String id = (String) user.get("id");
            List<String> keywords = this.tweetsIndexService.getKeywords(id);
            if (!this.usersIndexService.updateProfile(id, keywords)) {
                logger.error("ELASTIC-SEARCH-SERVICE: error during update profile " + id);
                return false;
            }
        }
        return true;
    }

    @Override
    public List<Map<String, Object>> search(String query, String hashtags, String mentions, boolean synonyms, boolean self, String id) {

        logger.info("ELASTIC-SEARCH-SERVICE: search");

        List<String> hashtagsList = new ArrayList<>();
        if (hashtags != null && !hashtags.isEmpty()) {
            hashtagsList = Arrays.asList(hashtags.split(","));
        }

        List<String> mentionsList = new ArrayList<>();
        if (mentions != null && !mentions.isEmpty()) {
            mentionsList = Arrays.asList(mentions.split(","));
        }

        SearchRequest searchRequest = new SearchRequest("tweets");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        if (query != null && !query.isEmpty()) {
            if (synonyms) {
                queryBuilder.must(QueryBuilders.queryStringQuery(query).defaultField("parsed_text").defaultOperator(Operator.AND).analyzer("custom_search_analyzer"));
            } else {
                queryBuilder.must(QueryBuilders.queryStringQuery(query).defaultField("parsed_text").defaultOperator(Operator.AND));
            }
        }

        if (!self) {
            if (id != null && !id.isEmpty()) {
                queryBuilder.mustNot(QueryBuilders.termQuery("user.id", id));
            }
        }

        if (id != null && !id.isEmpty() && !id.equals("empty")) {
            Map<String, Object> user = this.usersIndexService.getUser(id);
            @SuppressWarnings("unchecked")
            List<String> profile = (List<String>) user.get("profile");
            for (String keyword : profile) {
                queryBuilder.should(QueryBuilders.termQuery("parsed_text", keyword));
            }
        }

        if (hashtagsList.size() > 0) {
            for(String hashtag : hashtagsList) {
                if (hashtag != null && !hashtag.isEmpty()) {
                    queryBuilder.must(QueryBuilders.matchQuery("hashtags", hashtag.trim()).operator(Operator.AND));
                }
            }
        }

        if (mentionsList.size() > 0) {
            for (String mention : mentionsList) {
                if (mention != null && !mention.isEmpty()) {
                    queryBuilder.must(QueryBuilders.matchQuery("mentions", mention.trim()).operator(Operator.AND));
                }
            }
        }

        searchRequest.source(searchSourceBuilder.query(queryBuilder));

        List<Map<String, Object>> tweets = new ArrayList<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    Map<String, Object> hitMap = hit.getSourceAsMap();
                    hitMap.put("score", hit.getScore());
                    tweets.add(hitMap);
                }
            } else {
                logger.info("ELASTIC-SEARCH-SERVICE: bad response");
            }
            return tweets;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return tweets;
        }
    }

    @Override
    public List<Map<String, String>> makeUsers(String selected) {

        List<Map<String, String>> users = new ArrayList<>();

        Map<String, String > noPersonalization = new HashMap<>();
        noPersonalization.put("id", "empty");
        noPersonalization.put("name", "No personalization");
        if (selected == null) {
            noPersonalization.put("selected", "true");
        } else {
            noPersonalization.put("selected", "false");
        }

        users.add(noPersonalization);

        for(Map<String, Object> user: this.usersIndexService.getUsers()){
            Map<String, String > userFormat = new HashMap<>();
            userFormat.put("id", (String) user.get("id"));
            userFormat.put("name", (String) user.get("name"));
            if (selected != null && !selected.isEmpty()) {
                if(selected.equals(user.get("id"))) {
                    userFormat.put("selected", "true");
                } else {
                    userFormat.put("selected", "false");
                }
            } else {
                userFormat.put("selected", "false");
            }
            users.add(userFormat);
        }

        return users;
    }

    @Override
    public List<String> getTopHashtags() {
        return this.tweetsIndexService.getTopHashtags();
    }

    @Override
    public List<String> getTopMentions() {
        return this.tweetsIndexService.getTopMentions();
    }

}
