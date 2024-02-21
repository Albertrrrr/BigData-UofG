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
        // generate JavaRDD
        JavaRDD<ScoreDistanceMap> rdd = sD.toJavaRDD();

        JavaRDD<QueryResult> queryResultRDD = rdd.flatMap(new FlatMapFunction<ScoreDistanceMap, QueryResult>() {
            @Override
            public Iterator<QueryResult> call(ScoreDistanceMap scoreDistanceMap) throws Exception {
                List<QueryResult> results = new ArrayList<>();
                scoreDistanceMap.getScoreAndDistance().forEach((query, scores) -> {
                    if (scores.size() >= 2 && scores.get(1) > 0.5) {
                        QueryResult qr = new QueryResult(query, scoreDistanceMap.getTitle(), scores.get(0), scoreDistanceMap.getId(), scoreDistanceMap.getArticle());
                        results.add(qr);
                    }
                });
                return results.iterator();
            }
        });

        JavaPairRDD<String, List<QueryResult>> groupedAndSortedRDD = queryResultRDD
                .mapToPair(result -> new Tuple2<>(result.getQuery().getOriginalQuery(), result))
                .groupByKey()
                .mapToPair(group -> {
                    // 对每个组的结果进行过滤，过滤掉title为null的QueryResult，并根据分数降序排序
                    List<QueryResult> sortedList = StreamSupport.stream(group._2.spliterator(), false)
                            .filter(qr -> qr.getTitle() != null)
                            .sorted((qr1, qr2) -> qr2.getScore().compareTo(qr1.getScore()))
                            .collect(Collectors.toList());

                    // 手动去重并保证最多10个唯一标题的结果
                    List<QueryResult> uniqueTitleList = new ArrayList<>();
                    Set<String> titles = new HashSet<>();
                    for (QueryResult qr : sortedList) {
                        if (uniqueTitleList.size() >= 10) break; // 如果已经有10个结果，就停止
                        if (titles.add(qr.getTitle())) { // 尝试将标题添加到集合中，成功表示标题是唯一的
                            uniqueTitleList.add(qr); // 添加这个唯一标题的QueryResult到结果列表中
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

        List<DocumentRanking> documentRankingList = groupedAndSortedRDD.map(group -> {
            List<QueryResult> results = group._2;
            List<RankedResult> needResult = new ArrayList<>();
            Query resQuery = null;
            for (QueryResult result : results) {
                NewsArticle targetArticle = result.getArticle();
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
