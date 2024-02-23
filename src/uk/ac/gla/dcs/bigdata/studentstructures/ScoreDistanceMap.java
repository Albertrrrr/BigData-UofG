package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ScoreDistanceMap implements Serializable {
    private String id;
    private String title;
    private NewsArticle article; // Reference to the detailed article
    private Map<Query, List<Double>> scoreAndDistance; // Mapping from queries to scores and distances

    // Constructor with parameters to initialize the object
    public ScoreDistanceMap(String id, String title, NewsArticle article, Map<Query, List<Double>> scoreAndDistance) {
        this.id = id;
        this.title = title;
        this.article = article;
        this.scoreAndDistance = scoreAndDistance;
    }

    // No-argument constructor for flexibility in instantiation
    public ScoreDistanceMap() {
    }

    // Getter for the article ID
    public String getId() {
        return id;
    }

    // Setter for the article ID
    public void setId(String id) {
        this.id = id;
    }

    // Getter for the article title
    public String getTitle() {
        return title;
    }

    // Setter for the article title
    public void setTitle(String title) {
        this.title = title;
    }

    // Getter for the score and distance map
    public Map<Query, List<Double>> getScoreAndDistance() {
        return scoreAndDistance;
    }

    // Setter for the score and distance map
    public void setScoreAndDistance(Map<Query, List<Double>> scoreAndDistance) {
        this.scoreAndDistance = scoreAndDistance;
    }

    // Getter for the NewsArticle object
    public NewsArticle getArticle() {
        return article;
    }

    // Setter for the NewsArticle object
    public void setArticle(NewsArticle article) {
        this.article = article;
    }
}
