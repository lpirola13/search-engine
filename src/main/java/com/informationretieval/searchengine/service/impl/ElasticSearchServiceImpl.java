package com.informationretieval.searchengine.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.informationretieval.searchengine.service.ElasticSearchService;
import com.informationretieval.searchengine.service.TweetsIndexService;
import com.informationretieval.searchengine.service.UsersIndexService;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import twitter4j.Logger;
import twitter4j.Status;
import twitter4j.User;

import java.io.IOException;
import java.util.*;

@Service
public class ElasticSearchServiceImpl implements ElasticSearchService {

    private static final Logger logger = Logger.getLogger(ElasticSearchServiceImpl.class);
    private final RestHighLevelClient restHighLevelClient;
    private final TweetsIndexService tweetsIndexService;
    private final UsersIndexService usersIndexService;
    @Value("${bighugelabs.apikey}")
    private String apikey;

    @Autowired
    public ElasticSearchServiceImpl(RestHighLevelClient restHighLevelClient, TweetsIndexService tweetsIndexService, UsersIndexService usersIndexService) {
        this.restHighLevelClient = restHighLevelClient;
        this.tweetsIndexService = tweetsIndexService;
        this.usersIndexService = usersIndexService;
    }

    @Override
    @Async
    @SuppressWarnings("deprecation")
    public void reset() {

        logger.info("ELASTIC-SEARCH-SERVICE: reset");

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
                        logger.info("ELASTIC-SEARCH-SERVICE: end reset");
                    } else {
                        logger.error("ELASTIC-SEARCH-SERVICE: error during users create");
                    }
                } else {
                    logger.error("ELASTIC-SEARCH-SERVICE: error during users delete");
                }
            } else {
                logger.error("ELASTIC-SEARCH-SERVICE: error during tweets create");
            }
        } else {
            logger.error("ELASTIC-SEARCH-SERVICE: error during tweets delete");
        }
    }

    @Override
    @Async
    public void update() {

        logger.info("ELASTIC-SEARCH-SERVICE: update");

        List<Map<String, Object>> users = this.usersIndexService.getUsers();
        for (Map<String, Object> user : users) {
            String id = (String) user.get("id");
            List<String> keywords = this.tweetsIndexService.getKeywords(id);
            List<String> hashtags = this.tweetsIndexService.getHastags(id);
            if (!this.usersIndexService.update(id, keywords, hashtags)) {
                logger.error("ELASTIC-SEARCH-SERVICE: error during update profile " + id);
            }
        }
        logger.info("ELASTIC-SEARCH-SERVICE: end update");
    }

    @Override
    public List<Map<String, Object>> search(String query, String hashtags, String mentions, boolean synonyms, boolean self, String id, Date fromDate, Date toDate) {

        if (id != null) {
            logger.info("HOME-CONTROLLER: search user: " + id + " query: " + query + " hashtags: " + hashtags + " mentions: " + mentions + " synonyms: " + synonyms + " self: " + self);
        } else {
            logger.info("HOME-CONTROLLER: search query: " + query + " hashtags: " + hashtags + " mentions: " + mentions + " synonyms: " + synonyms + " self: " + self);
        }

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

        if (fromDate != null && toDate != null) {
            Calendar calendarFrom = Calendar.getInstance();
            calendarFrom.setTime(fromDate);
            calendarFrom.set(Calendar.HOUR_OF_DAY, 1);
            Calendar calendarTo = Calendar.getInstance();
            calendarTo.setTime(toDate);
            calendarTo.add(Calendar.DATE, 1);
            calendarTo.set(Calendar.HOUR_OF_DAY, 1);
            queryBuilder.must(QueryBuilders.rangeQuery("created_at").gte(calendarFrom.getTime()).lt(calendarTo.getTime()));
        }

        if (fromDate == null && toDate != null) {
            Calendar calendarTo = Calendar.getInstance();
            calendarTo.setTime(toDate);
            calendarTo.add(Calendar.DATE, 1);
            calendarTo.set(Calendar.HOUR_OF_DAY, 1);
            queryBuilder.must(QueryBuilders.rangeQuery("created_at").lt(calendarTo.getTime()));
        }

        if (fromDate != null && toDate == null) {
            Calendar calendarFrom = Calendar.getInstance();
            calendarFrom.setTime(fromDate);
            calendarFrom.set(Calendar.HOUR_OF_DAY, 1);
            queryBuilder.must(QueryBuilders.rangeQuery("created_at").gte(calendarFrom.getTime()));
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
            @SuppressWarnings("unchecked")
            List<String> profileHashtags = (List<String>) user.get("hashtags");
            queryBuilder.should(QueryBuilders.termsQuery("parsed_text", profile));
            queryBuilder.should(QueryBuilders.termsQuery("hashtags", profileHashtags));
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
    public List<Map<String, String>> getUsers(String selected) {

        logger.info("ELASTIC-SEARCH-SERVICE: getUsers selected: " + selected);

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

        logger.info("ELASTIC-SEARCH-SERVICE: getTopHashtags");

        return this.tweetsIndexService.getTopHashtags();
    }

    @Override
    public List<String> getTopMentions() {

        logger.info("ELASTIC-SEARCH-SERVICE: getTopMentions");

        return this.tweetsIndexService.getTopMentions();
    }

    @Override
    @Async
    public void synonyms() {

        logger.info("ELASTIC-SEARCH-SERVICE: synonyms");

        ObjectMapper mapper = new ObjectMapper();

        List<String> synonyms = new ArrayList<>();

        List<Map<String, Object>> users = this.usersIndexService.getUsers();
        for (Map<String, Object> user : users) {

            List<String> keywords = this.tweetsIndexService.getKeywords((String) user.get("id"));

            for (String keyword : keywords) {

                String finalString = keyword;

                if (keyword.length() > 1) {

                    try (CloseableHttpClient client = HttpClients.createDefault()) {

                        HttpGet thesaurusRequest = new HttpGet("https://words.bighugelabs.com/api/2/" + apikey + "/" + keyword + "/json");

                        JsonNode thesaurusResponse = client.execute(thesaurusRequest, httpResponse -> mapper.readTree(httpResponse.getEntity().getContent()));

                        if (thesaurusResponse.get("noun") != null) {
                            if (thesaurusResponse.get("noun").get("syn") != null) {
                                for (JsonNode synonymJSON : thesaurusResponse.get("noun").get("syn")) {
                                    finalString = finalString.concat(", ").concat(synonymJSON.asText());
                                }
                                logger.info(finalString);
                                synonyms.add(finalString);
                            }
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }

        }
        if (this.tweetsIndexService.synonyms(synonyms)) {
            logger.error("ELASTIC-SEARCH-SERVICE: error during updating synonyms");
        }
    }

    @Override
    public boolean indexUser(User user) {

        logger.info("ELASTIC-SEARCH-SERVICE: indexUser");

        return this.usersIndexService.index(user);
    }

    @Override
    public boolean indexTweets(List<Status> statuses) {

        logger.info("ELASTIC-SEARCH-SERVICE: indexTweets");

        return this.tweetsIndexService.index(statuses);
    }

}
