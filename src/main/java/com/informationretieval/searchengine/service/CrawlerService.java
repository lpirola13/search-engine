package com.informationretieval.searchengine.service;

import java.net.URI;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.informationretieval.searchengine.model.Tweet;
import com.informationretieval.searchengine.model.TwitterUser;
import com.vdurmont.emoji.EmojiParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class CrawlerService {

    private static final Logger logger = Logger.getLogger(CrawlerService.class);
    private IndexService indexService;

    @Autowired
    public CrawlerService(IndexService indexService) {
        this.indexService = indexService;
    }

    @Async
    public void startCrawler() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        Gson gson = new GsonBuilder().create();
        String path = "/home/tweets";

        Directory dir = null;
        try {
            Files.createDirectories(Paths.get("/home/index"));
            Files.createDirectories(Paths.get("/home/tweets"));
            dir = FSDirectory.open(Paths.get("/home/index"));
        } catch (IOException e) {
            logger.error("Failed to open index dir: " + e.getMessage());
        }

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(256.0);

        IndexWriter writerDoc = null;
        try {
            writerDoc = new IndexWriter(dir, iwc);
        } catch (IOException e) {
            logger.error("Failed to open writer: " + e.getMessage());
        }


        try {
            Files.createDirectories(Paths.get(path));
        } catch (IOException e) {
            logger.error("Failed to make tweets dir: " + e.getMessage());
        }
        try {
            String user = "Cristiano";
            logger.info("GET @" + user + "'s timeline");
            //for (int i = 1; i < 101; i++) {
            int i = 1;
                List<Status> statuses = twitter.getUserTimeline(user, new Paging(i, 10));
                logger.info("GET page " + i + " out of 100");
                TwitterUser twitterUser = new TwitterUser();
                twitterUser.setId(statuses.get(0).getUser().getId());
                twitterUser.setName(statuses.get(0).getUser().getName());
                twitterUser.setScreenName(statuses.get(0).getUser().getScreenName());
                for (Status status : statuses) {
                    if (!status.isRetweet()) {
                        logger.info("GET @" + status.getUser().getScreenName() + " - " + status.getId() + " - " + status.getCreatedAt());
                        Writer writer = new FileWriter(path + "/" + status.getId() + ".json");
                        Tweet tweet = new Tweet();
                        tweet.setId(status.getId());
                        tweet.setText(status.getText());
                        tweet.setCreatedAt(status.getCreatedAt());
                        tweet.setLang(status.getLang());
                        tweet.setUser(twitterUser);
                        gson.toJson(tweet, writer);
                        writer.flush();
                        writer.close();
                        Document doc = getDocument(tweet);
                        writerDoc.addDocument(doc);
                    }
                }
                TimeUnit.SECONDS.sleep(5);
                writerDoc.close();


           // }
        } catch (TwitterException | IOException | InterruptedException te) {
            logger.error("Failed to get timeline: " + te.getMessage());
        }
    }

    private Document getDocument(Tweet tweet) {
        Document doc = new Document();
        doc.add(new LongPoint("id", tweet.getId()));
        doc.add(new TextField("text", EmojiParser.parseToAliases(tweet.getText(), EmojiParser.FitzpatrickAction.REMOVE), Field.Store.YES));
        return doc;
    }

    public void searchDoc() {
        Directory dir = null;
        try {
            dir = FSDirectory.open(Paths.get("/home/index"));
        } catch (IOException e) {
            logger.error("Failed to open index dir: " + e.getMessage());
        }

        String field = "text";
        String searchFor = "feliz";
        int max_results = 10;

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer1 = new StandardAnalyzer();
        QueryBuilder queryBuilder = new QueryBuilder(analyzer1);

        Query phraseQuery = queryBuilder.createPhraseQuery(field, searchFor);

        TopDocs results = null;
        try {
            results = searcher.search(phraseQuery, max_results);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ScoreDoc[] hits = results.scoreDocs;

        logger.info("RESULTS");
        for (ScoreDoc docu : hits){
            logger.info("doc: " + docu.doc + " score: " + docu.score);
            Document doc = null;
            try {
                doc = searcher.doc(docu.doc);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("doc: " + doc.get("text"));
        }
    }



    public void testBonsai() {
        String host = "https://ys015aap85:rrsv9ev7bt@boxwood-659830520.eu-west-1.bonsaisearch.net:443";

        URI connUri = URI.create(host);
        String[] auth = connUri.getUserInfo().split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search.html
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        IndexRequest indexRequest = new IndexRequest("car")
                .id("1")
                .source("model", "BMW",
                        "year", 1920,
                        "engine", "electric engine i8");

        try {
            IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
            logger.info(indexResponse.getResult().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Need to close the client so the thread will exit
        try {
            restClient.close();
        } catch (Exception ex) {

        }
    }


    @Async
    public void crawlProfile() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        Gson gson = new GsonBuilder().create();
        String host = "https://ys015aap85:rrsv9ev7bt@boxwood-659830520.eu-west-1.bonsaisearch.net:443";
        URI connUri = URI.create(host);
        String[] auth = connUri.getUserInfo().split(":");
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        try {
            String user = "Cristiano";
            logger.info("GET @" + user + "'s timeline");
            //for (int i = 1; i < 101; i++) {
            int i = 1;
            List<Status> statuses = twitter.getUserTimeline(user, new Paging(i, 10));
            logger.info("GET page " + i + " out of 100");
            for (Status status : statuses) {
                if (!status.isRetweet()) {
                    logger.info("GET @" + status.getUser().getScreenName() + " - " + status.getId() + " - " + status.getCreatedAt());
                    IndexRequest indexRequest = new IndexRequest("tweets")
                            .id(String.valueOf(status.getId()))
                            .source("text", status.getText(),
                                    "created_at", status.getCreatedAt(),
                                    "lang", status.getLang(),
                                    "user", String.valueOf(status.getUser().getId()));
                    try {
                        IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info("PUT: " + indexResponse.getResult().toString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            //TimeUnit.SECONDS.sleep(5);
            restClient.close();

        } catch (TwitterException | IOException te) {
            logger.error("Failed to get timeline: " + te.getMessage());
        }
    }

}
