package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.UsersIndexService;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import twitter4j.*;

import java.io.IOException;
import java.util.*;

@Service
public class UsersIndexServiceImpl implements UsersIndexService {

    private static final Logger logger = Logger.getLogger(UsersIndexServiceImpl.class);
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public UsersIndexServiceImpl(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public boolean create() {
        logger.info("USERS-INDEX-SERVICE: create index");

        CreateIndexRequest request = new CreateIndexRequest("users");

        XContentBuilder mappings;
        try {
            mappings = XContentFactory.jsonBuilder();
            mappings.startObject()
                    .startObject("properties")
                    .startObject("id")
                    .field("type", "text")
                    .endObject()
                    .startObject("screen_name")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("name")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("profile")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("hashtags")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }

        XContentBuilder settings;
        try {
            settings = XContentFactory.jsonBuilder();
            settings.startObject()
                        .field("number_of_shards",1)
                        .field("number_of_replicas",0)
                    .endObject();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }

        request.mapping(mappings);
        request.settings(settings);

        try {
            return restHighLevelClient.indices().create(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    @Override
    public boolean delete() {
        logger.info("USERS-INDEX-SERVICE: delete index");

        DeleteIndexRequest request = new DeleteIndexRequest("users");

        try {
            return restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            logger.info(e.getMessage());
            return false;
        }

    }

    @Override
    public boolean index(User user) {

        logger.info("USERS-INDEX-SERVICE: index user: " + user.getScreenName());

        Map<String, String> userMap = new HashMap<>();
        userMap.put("id", String.valueOf(user.getId()));
        userMap.put("name", user.getName());
        userMap.put("screenName", user.getScreenName());

        IndexRequest request = new IndexRequest("users").id(String.valueOf(user.getId())).source(userMap);

        try {
            return restHighLevelClient.index(request, RequestOptions.DEFAULT).status().equals(RestStatus.CREATED);
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public List<Map<String, Object>> getUsers() {

        logger.info("USERS-INDEX-SERVICE: getUsers");

        SearchRequest searchRequest = new SearchRequest("users");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(searchSourceBuilder);
        List<Map<String, Object>> users = new ArrayList<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    Map<String, Object> user = hit.getSourceAsMap();
                    users.add(user);
                }
            } else {
                logger.error("USERS-INDEX-SERVICE: bad response");
            }
            return users;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return users;
        }
    }

    public boolean update(String id, List<String> keywords, List<String> hashtags){

        logger.info("USERS-INDEX-SERVICE: update user: " + id);

        Map<String, Object> user = new HashMap<>();
        user.put("profile", keywords);
        user.put("hashtags", hashtags);

        UpdateRequest request = new UpdateRequest("users", id).doc(user);

        try {
            UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                return true;
            } else {
                logger.error("USERS-INDEX-SERVICE: bad response");
                return false;
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public Map<String, Object> getUser(String id){

        logger.info("USERS-INDEX-SERVICE: getUser id: " + id);

        SearchRequest searchRequest = new SearchRequest("users");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchRequest.source(searchSourceBuilder.query(QueryBuilders.termQuery("id", id)));

        Map<String, Object> user = new HashMap<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                SearchHit[] hits = response.getHits().getHits();
                user = hits[0].getSourceAsMap();
            } else {
                logger.info("USERS-INDEX-SERVICE: bad response");
            }
            return user;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return user;
        }
    }

}
