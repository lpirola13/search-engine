package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.CrawlerService;
import com.informationretieval.searchengine.service.ElasticSearchService;
import com.informationretieval.searchengine.service.TweetsIndexService;
import com.informationretieval.searchengine.service.UsersIndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.List;

@Service
public class CrawlerServiceImpl implements CrawlerService {

    private static final Logger logger = Logger.getLogger(CrawlerServiceImpl.class);
    private final TweetsIndexService tweetsIndexService;
    private final UsersIndexService usersIndexService;
    private final ElasticSearchService elasticSearchService;
    @Value("${com.informationretrieval.users}")
    private String[] users;
    @Value("${com.informationretrieval.pages}")
    private int pages;

    @Autowired
    public CrawlerServiceImpl(TweetsIndexService tweetsIndexService, UsersIndexService usersIndexService, ElasticSearchService elasticSearchService) {
        this.tweetsIndexService = tweetsIndexService;
        this.usersIndexService = usersIndexService;
        this.elasticSearchService = elasticSearchService;
    }

    @Override
    @Async
    @SuppressWarnings("all")
    public void start(String user) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        logger.info("CRAWLER-SERVICE: start crawler");

        try {
            logger.info("CRAWLER-SERVICE: indexing user " + user);
            List<Status> firstStatus = twitter.getUserTimeline(user, new Paging(1, 1));
            if (!usersIndexService.index(firstStatus.get(0).getUser())) {
                logger.error("CRAWLER-SERVICE: error during indexing");
                return;
            }
            Thread.sleep(10000);
            logger.info("CRAWLER-SERVICE: getting @" + user + "'s timeline");
            for (int i = 1; i <= pages; i++) {
                logger.info("CRAWLER-SERVICE: get page " + i + " out of " + pages);
                List<Status> statuses = twitter.getUserTimeline(user, new Paging(i, 10));
                if (!tweetsIndexService.index(statuses)) {
                    logger.error("CRAWLER-SERVICE: error during indexing page " + i);
                    return;
                }
                Thread.sleep(10000);
            }
            logger.info("CRAWLER-SERVICE: end crawler");
        } catch (TwitterException | InterruptedException e) {
            logger.error(e.getMessage());
            return;
        }

    }

    @Override
    @Async
    public void reset() {

        logger.info("CRAWLER-SERVICE: reset indices");

        this.elasticSearchService.resetIndices();
    }

    @Override
    @Async
    public void update() {
        logger.info("CRAWLER-SERVICE: start update profiles");
        if (!this.elasticSearchService.updateUsersProfile()) {
            logger.error("CRAWLER-SERVICE: error during update profiles");
            return;
        }
        logger.info("CRAWLER-SERVICE: end update profiles");
    }

}
