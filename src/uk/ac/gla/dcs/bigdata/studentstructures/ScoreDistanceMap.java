package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ScoreDistanceMap implements Serializable {
    String id;
    String title;
    Map<Query, List<Double>> scoreAndDistance;

    public ScoreDistanceMap(String id, String title, Map<Query, List<Double>> scoreAndDistance) {
        this.id = id;
        this.title = title;
        this.scoreAndDistance = scoreAndDistance;
    }

    public ScoreDistanceMap() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Map<Query, List<Double>> getScoreAndDistance() {
        return scoreAndDistance;
    }

    public void setScoreAndDistance(Map<Query, List<Double>> scoreAndDistance) {
        this.scoreAndDistance = scoreAndDistance;
    }
}
