package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;

public class QueryResult implements Serializable {
    private Query query; // The query associated with this result
    private String title; // The title of the news article
    private Double score; // The relevance score of the article to the query
    private String id; // The unique identifier of the article
    private NewsArticle article; // The article object itself

    // Constructor
    public QueryResult(Query query, String title, Double score, String id, NewsArticle article) {
        this.query = query;
        this.title = title;
        this.score = score;
        this.id = id;
        this.article = article;
    }

    // Getter for the query
    public Query getQuery() {
        return query;
    }

    // Setter for the query
    public void setQuery(Query query) {
        this.query = query;
    }

    // Getter for the title
    public String getTitle() {
        return title;
    }

    // Setter for the title
    public void setTitle(String title) {
        this.title = title;
    }

    // Getter for the score
    public Double getScore() {
        return score;
    }

    // Setter for the score
    public void setScore(Double score) {
        this.score = score;
    }

    // Getter for the ID
    public String getId() {
        return id;
    }

    // Setter for the ID
    public void setId(String id) {
        this.id = id;
    }

    // Getter for the article
    public NewsArticle getArticle() {
        return article;
    }

    // Setter for the article
    public void setArticle(NewsArticle article) {
        this.article = article;
    }
}