package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.TweetsIndexService;
import com.vdurmont.emoji.EmojiParser;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.GND;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import twitter4j.*;

import java.io.IOException;
import java.util.*;

@Service
public class TweetsIndexServiceImpl implements TweetsIndexService {

    private static final Logger logger = Logger.getLogger(TweetsIndexServiceImpl.class);
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public TweetsIndexServiceImpl(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public boolean create() {

        logger.info("TWEETS-INDEX-SERVICE: create index");

        CreateIndexRequest request = new CreateIndexRequest("tweets");

        XContentBuilder mappings;
        try {
            mappings = XContentFactory.jsonBuilder();
            mappings.startObject()
                    .startObject("properties")
                    .startObject("text")
                    .field("type", "text")
                    .field("index", "false")
                    .endObject()
                    .startObject("parsed_text")
                    .field("type", "text")
                    .field("term_vector","yes")
                    .field("analyzer","custom_analyzer")
                    .endObject()
                    .startObject("created_at")
                    .field("type", "date")
                    .endObject()
                    .startObject("user")
                    .startObject("properties")
                    .startObject("id")
                    .field("type", "text")
                    .endObject()
                    .startObject("name")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("screenName")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("hashtags")
                    .field("type", "keyword")
                    .field("normalizer","custom_normalizer")
                    .endObject()
                    .startObject("mentions")
                    .field("type", "keyword")
                    .field("normalizer","custom_normalizer")
                    .endObject()
                    .startObject("urls")
                    .field("type", "keyword")
                    .field("normalizer","custom_normalizer")
                    .endObject()
                    .endObject()
                    .endObject();

        } catch (IOException e) {
            logger.info(e.getMessage());
            return false;
        }

        XContentBuilder settings;
        try {
            settings = XContentFactory.jsonBuilder();
            settings.startObject()
                    .field("number_of_shards",1)
                    .field("number_of_replicas",0)
                    .startObject("analysis")
                    .startObject("filter")
                    .startObject("synonym_filter")
                    .field("type", "synonym")
                    .startArray("synonyms")
                    .value("formula1, formula 1, f1")
                    .endArray()
                    .endObject()
                    .endObject()
                    .startObject("char_filter")
                    .startObject("char_filter_1")
                    .field("type", "pattern_replace")
                    .field("pattern", "(?=(:\\\\w+:))")
                    .field("replacement", " ")
                    .endObject()
                    .startObject("char_filter_2")
                    .field("type", "pattern_replace")
                    .field("pattern", "(_)")
                    .field("replacement", " ")
                    .endObject()
                    .endObject()
                    .startObject("analyzer")
                    .startObject("custom_analyzer")
                    .field("type", "custom")
                    .startArray("char_filter")
                    .value("char_filter_1")
                    .value("char_filter_2")
                    .endArray()
                    .field("tokenizer", "uax_url_email")
                    .startArray("filter")
                    .value("lowercase")
                    .value("apostrophe")
                    .value("stop")
                    .value("porter_stem")
                    .endArray()
                    .endObject()
                    .startObject("custom_search_analyzer")
                    .field("type", "custom")
                    .startArray("char_filter")
                    .value("char_filter_1")
                    .value("char_filter_2")
                    .endArray()
                    .field("tokenizer", "uax_url_email")
                    .startArray("filter")
                    .value("lowercase")
                    .value("apostrophe")
                    .value("stop")
                    .value("synonym_filter")
                    .value("porter_stem")
                    .endArray()
                    .endObject()
                    .endObject()
                    .startObject("normalizer")
                    .startObject("custom_normalizer")
                    .field("type", "custom")
                    .startArray("filter")
                    .value("lowercase")
                    .endArray()
                    .endObject()
                    .endObject()
                    .endObject()
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

        logger.info("TWEETS-INDEX-SERVICE: delete index");

        DeleteIndexRequest request = new DeleteIndexRequest("tweets");

        try {
            return restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            logger.info(e.getMessage());
            return false;
        }
    }

    public boolean index(List<Status> statuses){

        BulkRequest request = new BulkRequest();

        int total = 0;
        if (statuses.size() > 0) {
            for (Status status : statuses) {
                if (!status.isRetweet()) {
                    total++;
                    logger.info("TWEETS-INDEX-SERVICE: indexing document " + status.getId());
                    List<String> hashtags = new ArrayList<>();
                    for (HashtagEntity he : status.getHashtagEntities()) {
                        hashtags.add(he.getText());
                    }

                    List<String> mentions = new ArrayList<>();
                    for (UserMentionEntity ume : status.getUserMentionEntities()) {
                        mentions.add(ume.getScreenName());
                    }

                    List<String> urls = new ArrayList<>();
                    for (URLEntity urle : status.getURLEntities()) {
                        urls.add(urle.getExpandedURL());
                    }

                    Map<String, String> user = new HashMap<>();
                    user.put("id", String.valueOf(status.getUser().getId()));
                    user.put("name", status.getUser().getName());
                    user.put("screenName", status.getUser().getScreenName());

                    request.add(new IndexRequest("tweets")
                            .id(String.valueOf(status.getId()))
                            .source("text", status.getText(),
                                    "parsed_text", EmojiParser.parseToAliases(status.getText(),
                                            EmojiParser.FitzpatrickAction.REMOVE),
                                    "created_at", status.getCreatedAt(),
                                    "hashtags", hashtags,
                                    "mentions", mentions,
                                    "urls", urls,
                                    "user", user));
                }
            }
            try {
                if(total>0) {
                    logger.info("TWEETS-INDEX-SERVICE: indexing " + total + " documents");
                    BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
                    return !response.hasFailures();
                } else {
                    logger.info("TWEETS-INDEX-SERVICE: no documents indexed");
                    return true;
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
                return false;
            }
        } else {
            logger.info("TWEETS-INDEX-SERVICE: no statuses");
            return true;
        }
    }


    public List<String> getTopHashtags() {

        logger.info("TWEETS-INDEX-SERVICE: get top hashtags");

        SearchRequest searchRequest = new SearchRequest("tweets");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).aggregation(AggregationBuilders.terms("top_hashtags").field("hashtags").size(10));
        searchRequest.source(searchSourceBuilder);

        List<String> hashtags = new ArrayList<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                Terms topHashtags = response.getAggregations().get("top_hashtags");
                hashtags = buildTermsList(topHashtags);
            } else {
                logger.error("TWEETS-INDEX-SERVICE: bad response");
            }
            return hashtags;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return hashtags;
        }
    }

    public List<String> getTopMentions() {

        logger.info("TWEETS-INDEX-SERVICE: get top mentions");

        SearchRequest searchRequest = new SearchRequest("tweets");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).aggregation(AggregationBuilders.terms("top_mentions").field("mentions").size(10));
        searchRequest.source(searchSourceBuilder);

        List<String> mentions = new ArrayList<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                Terms topMentions = response.getAggregations().get("top_mentions");
                mentions = buildTermsList(topMentions);
            } else {
                logger.error("TWEETS-INDEX-SERVICE: bad response");
            }
            return mentions;
        } catch (IOException e) {
             logger.error(e.getMessage());
            return mentions;
        }
    }

    public List<String> getKeywords(String id){

        logger.info("TWEETS-INDEX-SERVICE: get keywords user " + id);

        SearchRequest searchRequest = new SearchRequest("tweets");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.termQuery("user.id", id));
        searchSourceBuilder.size(0).aggregation(AggregationBuilders.significantText("keywords", "parsed_text").size(20).significanceHeuristic(new GND(false)));

        searchRequest.source(searchSourceBuilder);

        List<String> keywords = new ArrayList<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                ParsedSignificantStringTerms hits = response.getAggregations().get("keywords");
                for (SignificantTerms.Bucket bucket: hits.getBuckets()) {
                    keywords.add(bucket.getKey().toString());
                }
            } else {
                logger.error("TWEETS-INDEX-SERVICE: bad response");
            }
            return keywords;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return keywords;
        }
    }

    private List<String> buildTermsList(Terms terms) {
        List<String> termsList = new ArrayList<>();

        for (Terms.Bucket entry : terms.getBuckets()) {
            termsList.add((String) entry.getKey());
        }

        return termsList;
    }

}
