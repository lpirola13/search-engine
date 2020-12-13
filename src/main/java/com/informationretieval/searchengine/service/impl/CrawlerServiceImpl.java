package com.informationretieval.searchengine.service.impl;

import com.informationretieval.searchengine.service.CrawlerService;
import com.informationretieval.searchengine.service.ElasticSearchService;
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
    private final ElasticSearchService elasticSearchService;
    @Value("${com.informationretrieval.pages}")
    private int pages;

    @Autowired
    public CrawlerServiceImpl(ElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }

    @Override
    @Async
    @SuppressWarnings("all")
    public void start(String user) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        logger.info("CRAWLER-SERVICE: start");

        try {
            logger.info("CRAWLER-SERVICE: user " + user);
            List<Status> firstStatus = twitter.getUserTimeline(user, new Paging(1, 1));
            if (!elasticSearchService.indexUser(firstStatus.get(0).getUser())) {
                logger.error("CRAWLER-SERVICE: error during indexing");
                return;
            }
            Thread.sleep(10000);
            logger.info("CRAWLER-SERVICE: " + user + " timeline");
            for (int i = 1; i <= pages; i++) {
                logger.info("CRAWLER-SERVICE: get page " + i + " out of " + pages);
                List<Status> statuses = twitter.getUserTimeline(user, new Paging(i, 10));
                if (!elasticSearchService.indexTweets(statuses)) {
                    logger.error("CRAWLER-SERVICE: error during indexing page " + i);
                    return;
                }
                Thread.sleep(10000);
            }
            logger.info("CRAWLER-SERVICE: end start");
        } catch (TwitterException | InterruptedException e) {
            logger.error(e.getMessage());
            return;
        }

    }
}
