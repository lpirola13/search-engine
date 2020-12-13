package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;

@Service
public interface CrawlerService {

    void start(String user);

    void reset();

    void update();
}
