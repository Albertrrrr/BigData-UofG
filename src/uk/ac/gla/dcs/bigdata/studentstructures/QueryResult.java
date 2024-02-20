package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;

public class QueryResult implements Serializable {
    private Query query;
    private String title;
    private Double score;
    private String id;

    public QueryResult(Query query, String title, Double score, String id) {
        this.query = query;
        this.title = title;
        this.score = score;
        this.id = id;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
