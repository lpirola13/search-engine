package com.informationretieval.searchengine.service;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import twitter4j.Logger;
import twitter4j.Status;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;

@Service
public class DocumentService {

    private static final Logger logger = Logger.getLogger(DocumentService.class);

    @Value("${bonsai.url}")
    private String bonsaiUrl;


    public boolean indexDocuments(List<Status> statuses) throws IOException {

        logger.info("DOCUMENT-SERVICE: indexing documents");

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

        BulkRequest request = new BulkRequest();
        for (Status status : statuses) {
            request.add(new IndexRequest("twitter")
                    .id(String.valueOf(status.getId()))
                    .source("text", status.getText(),
                            "created_at", status.getCreatedAt(),
                            "lang", status.getLang(),
                            "user", String.valueOf(status.getUser().getId())));
        }

        BulkResponse bulkResponse = restClient.bulk(request, RequestOptions.DEFAULT);

        return bulkResponse.hasFailures();
    }
}
