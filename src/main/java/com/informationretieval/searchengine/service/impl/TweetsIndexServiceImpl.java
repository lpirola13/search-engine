package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.TweetsIndexService;
import com.vdurmont.emoji.EmojiParser;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import twitter4j.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        logger.info("TWEETS-INDEX-SERVICE: create");

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
                    .field("similarity","my_similarity")
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
                    .startObject("similarity")
                    .startObject("my_similarity")
                    .field("type", "LMDirichlet")
                    .endObject()
                    .endObject()
                    .startObject("analysis")
                    .startObject("filter")
                    .startObject("synonym_filter")
                    .field("type", "synonym")
                    .startArray("synonyms_graph")
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

        logger.info("TWEETS-INDEX-SERVICE: delete");

        DeleteIndexRequest request = new DeleteIndexRequest("tweets");

        try {
            return restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            logger.info(e.getMessage());
            return false;
        }
    }

    public boolean index(List<Status> statuses){

        logger.info("TWEETS-INDEX-SERVICE: index");

        BulkRequest request = new BulkRequest();

        int total = 0;
        if (statuses.size() > 0) {
            for (Status status : statuses) {
                if (!status.isRetweet()) {
                    total++;
                    logger.info("TWEETS-INDEX-SERVICE: indexing document " + status.getId());

                    Map<String, String> user = new HashMap<>();
                    user.put("id", String.valueOf(status.getUser().getId()));
                    user.put("name", status.getUser().getName());
                    user.put("screenName", status.getUser().getScreenName());

                    List<String> hashtags = new ArrayList<>();
                    Matcher m1 = Pattern.compile("\\B#\\w\\w+").matcher(status.getText());
                    while (m1.find()) {
                        hashtags.add(m1.group());

                    }

                    List<String> mentions = new ArrayList<>();
                    Matcher m2 = Pattern.compile("(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9-_]+)").matcher(status.getText());
                    while (m2.find()) {
                        mentions.add(m2.group());
                    }

                    request.add(new IndexRequest("tweets")
                            .id(String.valueOf(status.getId()))
                            .source("text", status.getText(),
                                    "parsed_text", EmojiParser.parseToAliases(status.getText(),
                                            EmojiParser.FitzpatrickAction.REMOVE),
                                    "created_at", status.getCreatedAt(),
                                    "hashtags", hashtags,
                                    "mentions", mentions,
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

        logger.info("TWEETS-INDEX-SERVICE: getTopHashtags");

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

        logger.info("TWEETS-INDEX-SERVICE: getTopMentions");

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

        logger.info("TWEETS-INDEX-SERVICE: getKeywords id: " + id);

        SearchRequest searchRequest = new SearchRequest("tweets");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.termQuery("user.id", id));
        searchSourceBuilder.size(0).aggregation(AggregationBuilders.significantText("keywords", "parsed_text").size(30).significanceHeuristic(new JLHScore()));

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

    @Override
    public List<String> getHastags(String id) {

        logger.info("TWEETS-INDEX-SERVICE: getHashtags id: " + id);

        SearchRequest searchRequest = new SearchRequest("tweets");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).query(QueryBuilders.termQuery("user.id", id)).aggregation(AggregationBuilders.terms("hashtags").field("hashtags").size(10));
        searchRequest.source(searchSourceBuilder);

        List<String> hashtags = new ArrayList<>();

        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status().equals(RestStatus.OK)) {
                Terms topHashtags = response.getAggregations().get("hashtags");
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

    @Override
    public boolean synonyms(List<String> synonyms) {
        logger.info("TWEETS-INDEX-SERVICE: synonyms");

        logger.info("TWEETS-INDEX-SERVICE: closing tweets index");
        try {
            if (restHighLevelClient.indices().close(new CloseIndexRequest("tweets"), RequestOptions.DEFAULT).isAcknowledged()) {
                logger.info("TWEETS-INDEX-SERVICE: updating synonyms");

                UpdateSettingsRequest request = new UpdateSettingsRequest("tweets");
                XContentBuilder settings;
                try {
                    settings = XContentFactory.jsonBuilder();
                    settings.startObject()
                            .startObject("analysis")
                            .startObject("filter")
                            .startObject("synonym_filter")
                            .field("type", "synonym_graph")
                            .startArray("synonyms");

                    for (String synonym : synonyms) {
                        settings.value(synonym);
                    }

                    settings.endArray()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    return true;
                }

                request.settings(Strings.toString(settings), XContentType.JSON);

                try {
                    if (restHighLevelClient.indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged()) {
                        logger.info("TWEETS-INDEX-SERVICE: opening tweets index");
                        try {
                            restHighLevelClient.indices().open(new OpenIndexRequest("tweets"), RequestOptions.DEFAULT);
                            return false;
                        } catch (IOException e) {
                            logger.error(e.getMessage());
                            return true;
                        }
                    } else {
                        logger.error("TWEETS-INDEX-SERVICE: no acknowledged");
                        logger.info("TWEETS-INDEX-SERVICE: opening tweets index");
                        try {
                            restHighLevelClient.indices().open(new OpenIndexRequest("tweets"), RequestOptions.DEFAULT);
                            return false;
                        } catch (IOException e) {
                            logger.error(e.getMessage());
                            return true;
                        }
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    return true;
                }
            } else {
                logger.error("TWEETS-INDEX-SERVICE: no acknowledged");
                return true;
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            return true;
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
