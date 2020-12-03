package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.CrawlerService;
import com.informationretieval.searchengine.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.List;

@Service
public class CrawlerServiceImpl implements CrawlerService {

    private static final Logger logger = Logger.getLogger(CrawlerServiceImpl.class);
    private final IndexService indexService;
    @Value("${com.informationretrieval.users}")
    private String[] users;

    @Autowired
    public CrawlerServiceImpl(IndexService indexService) {
        this.indexService = indexService;
    }

    @Async
    public void start() {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        logger.info("CRAWLER-SERVICE: start crawler");

        try {
            for (String user : users) {
                logger.info("CRAWLER-SERVICE: getting @" + user + "'s timeline");
                for (int i = 1; i < 10; i++) {
                    logger.info("CRAWLER-SERVICE: get page " + i + " out of 100");
                    List<Status> statuses = twitter.getUserTimeline(user, new Paging(i, 10));
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        logger.error("CRAWLER-SERVICE: error during sleep");
                    }
                    boolean failures = indexService.index(statuses);
                    if (failures) {
                        logger.error("CRAWLER-SERVICE: error during indexing");
                    }
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.error("CRAWLER-SERVICE: error during sleep");
                    }
                }
            }
            logger.info("CRAWLER-SERVICE: stop crawler");
        } catch (TwitterException e) {
            logger.error("CRAWLER-SERVICE: error during tweets crawling");
        } catch (IOException e) {
            logger.error("CRAWLER-SERVICE: error during indexing");
        }
    }

}
