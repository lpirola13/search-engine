package com.informationretieval.searchengine.service;

import org.springframework.stereotype.Service;
import twitter4j.Status;
import twitter4j.User;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public interface UsersIndexService {

    boolean create();
    boolean delete();
    boolean index(User user);
    List<Map<String, Object>> getUsers();
    boolean updateProfile(String id, List<String> keywords);
    Map<String, Object> getUser(String id);

}
