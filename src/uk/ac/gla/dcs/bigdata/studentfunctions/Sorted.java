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
import java.util.Map;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.function.Function;
import static org.apache.spark.sql.functions.col;

public class Sorted implements Serializable {

    public List<DocumentRanking> ranking (SparkSession spark, Dataset<ScoreDistanceMap> sD, Dataset<NewsArticle> news){
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
                    // 对每个组的结果进行过滤、排序并限制为前10个，过滤掉title为null的QueryResult
                    List<QueryResult> filteredSortedAndLimitedList = StreamSupport.stream(group._2.spliterator(), false)
                            .filter(qr -> qr.getTitle() != null) // 过滤掉title为null的结果
                            .sorted((qr1, qr2) -> qr2.getScore().compareTo(qr1.getScore())) // 根据分数降序排序
                            .limit(10) // 限制结果数量为10
                            .collect(Collectors.toList());
                    return new Tuple2<>(group._1, filteredSortedAndLimitedList);
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

        List<NewsArticle> newsList = news.collectAsList();
        Map<String, NewsArticle> newsMap = newsList.stream().collect(Collectors.toMap(NewsArticle::getId, Function.identity()));

        List<DocumentRanking> documentRankingList = groupedAndSortedRDD.map(group -> {
            List<QueryResult> results = group._2;
            List<RankedResult> needResult = new ArrayList<>();
            Query resQuery = null;
            for (QueryResult result : results) {
                String targetId = result.getId();
                NewsArticle targetArticle = newsMap.get(targetId); // 从映射中获取NewsArticle对象
                if (targetArticle != null) {
                    needResult.add(new RankedResult(result.getId(), targetArticle, result.getScore()));
                    resQuery = result.getQuery();
                }
            }

            return new DocumentRanking(resQuery, needResult); // 返回每个组的DocumentRanking对象
        }).collect(); // 收集所有DocumentRanking对象到Driver节点

        return documentRankingList;
    }
}
