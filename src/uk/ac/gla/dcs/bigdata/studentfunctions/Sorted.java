package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleNeeded;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryResult;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoreDistanceMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Sorted implements Serializable {

    public Dataset<DocumentRanking> ranking (SparkSession spark, Dataset<ScoreDistanceMap> sD){
        JavaRDD<ScoreDistanceMap> rdd = sD.toJavaRDD();

        JavaRDD<QueryResult> queryResultRDD = rdd.flatMap(new FlatMapFunction<ScoreDistanceMap, QueryResult>() {
            @Override
            public Iterator<QueryResult> call(ScoreDistanceMap scoreDistanceMap) throws Exception {
                List<QueryResult> results = new ArrayList<>();
                scoreDistanceMap.getScoreAndDistance().forEach((query, scores) -> {
                    if (scores.size() >= 2 && scores.get(1) > 0.5) {
                        QueryResult qr = new QueryResult(query, scoreDistanceMap.getTitle(), scores.get(0), scoreDistanceMap.getId());
                        results.add(qr);
                    }
                });
                return results.iterator();
            }
        });

        JavaPairRDD<String, List<QueryResult>> groupedAndSortedRDD = queryResultRDD
                .mapToPair(result -> new Tuple2<>(result.getQuery().getOriginalQuery(), result)) // 使用getOriginalQuery()作为键
                .groupByKey()
                .mapToPair(group -> {
                    // 对每个组的结果进行排序并限制为前10个
                    List<QueryResult> sortedAndLimitedList = StreamSupport.stream(group._2.spliterator(), false)
                            .sorted((qr1, qr2) -> qr2.getScore().compareTo(qr1.getScore()))
                            .limit(10)
                            .collect(Collectors.toList());
                    return new Tuple2<>(group._1, sortedAndLimitedList);
                });

        groupedAndSortedRDD.foreach(group -> {
            String originalQuery = group._1;
            List<QueryResult> results = group._2;
            System.out.println("Original Query: " + originalQuery);
            for (QueryResult result : results) {
                System.out.println("\tScore: " + result.getScore() + ", Title: " + result.getTitle());
            }
            System.out.println("--------------------------------------------------");
        });



        return null;
    }
}
