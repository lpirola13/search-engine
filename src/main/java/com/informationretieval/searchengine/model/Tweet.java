package com.informationretieval.searchengine.model;

import java.util.Date;

public class Tweet {

    private Long id;
    private String text;
    private String lang;
    private Date createdAt;
    private TwitterUser twitterUser;

    public Tweet() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public TwitterUser getUser() {
        return twitterUser;
    }

    public void setUser(TwitterUser twitterUser) {
        this.twitterUser = twitterUser;
    }
}
