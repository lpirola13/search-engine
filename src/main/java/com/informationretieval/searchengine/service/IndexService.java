package com.informationretieval.searchengine.service;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import twitter4j.Logger;

import java.io.IOException;
import java.net.URI;

@Service
public class IndexService {

    private static final Logger logger = Logger.getLogger(IndexService.class);

    @Value("${bonsai.url}")
    private String bonsaiUrl;


    public boolean createIndex() throws IOException {

        logger.info("INDEX-SERVICE: creating new index");

        URI connUri = URI.create(bonsaiUrl);
        String[] auth = connUri.getUserInfo().split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                                httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        CreateIndexRequest request = new CreateIndexRequest("twitter");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("text");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
                builder.startObject("lang");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
                builder.startObject("created_at");
                {
                    builder.field("type", "date");
                }
                builder.endObject();
                builder.startObject("user");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder);

        ClearIndicesCacheRequest requestAll = new ClearIndicesCacheRequest();

        CreateIndexResponse createIndexResponse = restClient.indices().create(request, RequestOptions.DEFAULT);

        restClient.close();

        return createIndexResponse.isAcknowledged();

    }

    public boolean deleteIndex() throws IOException {

        logger.info("INDEX-SERVICE: deleting index");

        URI connUri = URI.create(bonsaiUrl);
        String[] auth = connUri.getUserInfo().split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                                httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        ClearIndicesCacheRequest requestAll = new ClearIndicesCacheRequest();

        DeleteIndexRequest request = new DeleteIndexRequest("twitter");
        AcknowledgedResponse deleteIndexResponse = restClient.indices().delete(request, RequestOptions.DEFAULT);

        restClient.close();

        return deleteIndexResponse.isAcknowledged();
    }
}
