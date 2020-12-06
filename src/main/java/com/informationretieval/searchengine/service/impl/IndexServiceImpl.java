package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.IndexService;
import com.vdurmont.emoji.EmojiParser;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import twitter4j.*;

import java.io.IOException;
import java.util.*;

@Service
public class IndexServiceImpl implements IndexService {

    private static final Logger logger = Logger.getLogger(IndexServiceImpl.class);
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public IndexServiceImpl(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    public boolean create() throws IOException {

        logger.info("INDEX-SERVICE: create index");

        CreateIndexRequest request = new CreateIndexRequest("twitter");

        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
        );

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
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
                        .startObject("lang")
                            .field("type", "keyword")
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

        request.mapping(builder);

        XContentBuilder settings = XContentFactory.jsonBuilder();
        settings.startObject()
                    .field("number_of_shards",1)
                    .field("number_of_replicas",0)
                    .startObject("analysis")
                        .startObject("analyzer")
                            .startObject("custom_analyzer")
                                .field("type", "custom")
                                .startObject("char_filter")
                                    .field("type", "pattern_replace")
                                    .field("pattern", "(?=(:\\\\w+:))")
                                    .field("replacement", " ")
                                .endObject()
                                .field("tokenizer", "uax_url_email")
                                .startArray("filter")
                                    .value("apostrophe")
                                    .value("stop")
                                    .value("lowercase")
                                .endArray()
                            .endObject()
                        .endObject()
                        .startObject("normalizer")
                            .startObject("custom_normalizer")
                                .field("type", "custom")
                                .startArray("filter")
                                    .value("lowercase")
                                    .value("porter_stem")
                                .endArray()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        request.settings(settings);

        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        return response.isAcknowledged();

    }

    public boolean delete() throws IOException {

        logger.info("INDEX-SERVICE: delete index");

        DeleteIndexRequest request = new DeleteIndexRequest("twitter");

        AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);

        return response.isAcknowledged();
    }

    public boolean index(List<Status> statuses) throws IOException {

        logger.info("DOCUMENT-SERVICE: index documents");

        BulkRequest request = new BulkRequest();

        logger.info("DOCUMENT-SERVICE: indexing " + statuses.size() + " documents");

        if (statuses.size() > 0) {
            for (Status status : statuses) {
                if (!status.isRetweet()) {
                    logger.info("DOCUMENT-SERVICE: indexing document " + status.getId());
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

                    request.add(new IndexRequest("twitter")
                            .id(String.valueOf(status.getId()))
                            .source("text", status.getText(),
                                    "parsed_text", EmojiParser.parseToAliases(status.getText(), EmojiParser.FitzpatrickAction.REMOVE),
                                    "created_at", status.getCreatedAt(),
                                    "lang", status.getLang(),
                                    "hashtags", hashtags,
                                    "mentions", mentions,
                                    "urls", urls,
                                    "user", user));
                }
            }

            BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);

            return response.hasFailures();
        }

        return true;
    }

    public List<Map<String, Object>> search(String query, String hashtags, String mentions) throws IOException {

        logger.info("INDEX-SERVICE: search index");

        List<String> hashtagsList = new ArrayList<>();
        if (hashtags != null && !hashtags.isEmpty()) {
            hashtagsList = Arrays.asList(hashtags.split(","));
        }

        List<String> mentionsList = new ArrayList<>();
        if (mentions != null && !mentions.isEmpty()) {
            mentionsList = Arrays.asList(mentions.split(","));
        }

        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        if (query != null && !query.isEmpty()) {
            queryBuilder.must(QueryBuilders.queryStringQuery(query).field("parsed_text"));
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

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        List<Map<String, Object>> hitsList = new ArrayList<>();

        if (response.status().equals(RestStatus.OK)) {
            SearchHits hits = response.getHits();
            TotalHits totalHits = hits.getTotalHits();
            if (totalHits.value > 0) {
                SearchHit[] searchHits = hits.getHits();
                for (SearchHit hit : searchHits) {
                    Map<String, Object> hitMap = hit.getSourceAsMap();
                    hitMap.put("score", hit.getScore());
                    hitsList.add(hitMap);
                }
            }
        } else {
            logger.info("INDEX-SERVICE: bad response");
        }

        return hitsList;
    }

    public List<String> getTopHashtags() throws IOException {

        logger.info("INDEX-SERVICE: get top hashtags");

        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).aggregation(AggregationBuilders.terms("top_hashtags").field("hashtags").size(10));
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Terms topHashtags = response.getAggregations().get("top_hashtags");

        List<String> hashtags = new ArrayList<>();

        if (response.status().equals(RestStatus.OK)) {
            hashtags = buildTermsList(topHashtags);
        } else {
            logger.info("INDEX-SERVICE: bad response");
        }

        return hashtags;
    }

    public List<String> getTopMentions() throws IOException {

        logger.info("INDEX-SERVICE: get top mentions");

        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).aggregation(AggregationBuilders.terms("top_mentions").field("mentions").size(10));
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Terms topMentions = response.getAggregations().get("top_mentions");

        List<String> mentions = new ArrayList<>();

        if (response.status().equals(RestStatus.OK)) {
            mentions = buildTermsList(topMentions);
        } else {
            logger.info("INDEX-SERVICE: bad response");
        }

        return mentions;
    }

    private List<String> buildTermsList(Terms terms) {
        List<String> termsList = new ArrayList<>();

        for (Terms.Bucket entry : terms.getBuckets()) {
            termsList.add((String) entry.getKey());
        }

        return termsList;
    }

}
