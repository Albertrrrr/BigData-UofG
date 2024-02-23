package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleNeeded;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryResult;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoreDistanceMap;
import java.util.*;
import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.function.Function;
import static org.apache.spark.sql.functions.col;

public class Sorted implements Serializable {

    public List<DocumentRanking> ranking (Dataset<ScoreDistanceMap> sD){
        // Convert Dataset to JavaRDD for Spark operations
        JavaRDD<ScoreDistanceMap> rdd = sD.toJavaRDD();

        // FlatMap operation to process each ScoreDistanceMap and generate QueryResult objects
        JavaRDD<QueryResult> queryResultRDD = rdd.flatMap(new FlatMapFunction<ScoreDistanceMap, QueryResult>() {
            @Override
            public Iterator<QueryResult> call(ScoreDistanceMap scoreDistanceMap) throws Exception {
                List<QueryResult> results = new ArrayList<>();
                // Process each score-distance mapping to generate query results
                scoreDistanceMap.getScoreAndDistance().forEach((query, scores) -> {
                    if (scores.size() >= 2 && scores.get(1) > 0.5) {
                        // Create and add new QueryResult based on filtering criteria
                        QueryResult qr = new QueryResult(query, scoreDistanceMap.getTitle(), scores.get(0), scoreDistanceMap.getId(), scoreDistanceMap.getArticle());
                        results.add(qr);
                    }
                });
                return results.iterator();
            }
        });

        // Transform to PairRDD, group by query, and sort/filter within each group
        JavaPairRDD<String, List<QueryResult>> groupedAndSortedRDD = queryResultRDD
                .mapToPair(result -> new Tuple2<>(result.getQuery().getOriginalQuery(), result))
                .groupByKey()
                .mapToPair(group -> {
                    // Filter the results for each group, filtering out QueryResults with null titles and sorting them in descending order by score
                    List<QueryResult> sortedList = StreamSupport.stream(group._2.spliterator(), false)
                            .filter(qr -> qr.getTitle() != null)
                            .sorted((qr1, qr2) -> qr2.getScore().compareTo(qr1.getScore()))
                            .collect(Collectors.toList());

                    // Further process to ensure uniqueness and limit the number of results
                    List<QueryResult> uniqueTitleList = new ArrayList<>();
                    Set<String> titles = new HashSet<>();
                    for (QueryResult qr : sortedList) {
                        if (uniqueTitleList.size() >= 10) break; // Limit to top 10 results
                        if (titles.add(qr.getTitle())) { // Ensure title uniqueness
                            uniqueTitleList.add(qr); // Add this uniquely titled QueryResult to the list of results
                        }
                    }

                    return new Tuple2<>(group._1, uniqueTitleList);
                });

        // check Results within RankingDocument
        //        groupedAndSortedRDD.foreach(group -> {
        //            String originalQuery = group._1;
        //            List<QueryResult> results = group._2;
        //            System.out.println("Original Query: " + originalQuery);
        //            for (QueryResult result : results) {
        //                System.out.println("\tScore: " + result.getScore() + ", Title: " + result.getTitle());
        //            }
        //            System.out.println("--------------------------------------------------");
        //        });

        // Final transformation to generate DocumentRanking objects for each group
        List<DocumentRanking> documentRankingList = groupedAndSortedRDD.map(group -> {
            List<QueryResult> results = group._2;
            List<RankedResult> needResult = new ArrayList<>();
            Query resQuery = null;
            // Create RankedResult objects for inclusion in DocumentRanking
            for (QueryResult result : results) {
                NewsArticle targetArticle = result.getArticle();
                if (targetArticle != null) {
                    needResult.add(new RankedResult(result.getId(), targetArticle, result.getScore()));
                    resQuery = result.getQuery();
                }
            }
            // Construct and return a DocumentRanking object for the group
            return new DocumentRanking(resQuery, needResult);
        }).collect(); // Collect all DocumentRanking objects to the Driver

        // Return the final list of DocumentRanking objects
        return documentRankingList;
    }
}
