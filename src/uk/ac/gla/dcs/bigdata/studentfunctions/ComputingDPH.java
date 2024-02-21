package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleNeeded;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoreDistanceMap;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Double.NaN;

public class ComputingDPH implements Serializable {
    private static final long serialVersionUID = 3905684103776472843L;
    public Dataset<ScoreDistanceMap> computingDPHScoreAndDistance (SparkSession spark, Dataset<Query> queries, Dataset<ArticleNeeded> news_Filter ){

        // Computing
        List<Query> queriesList = queries.collectAsList();

        // 广播查询集
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        Broadcast<List<Query>> queriesBroadcast = sparkContext.broadcast(queriesList);

        // 计算文档长度的Dataset
        Dataset<Integer> documentLengths = news_Filter.map(new MapFunction<ArticleNeeded, Integer>() {
            @Override
            public Integer call(ArticleNeeded article) throws Exception {
                return article.getContents().stream()
                        .flatMap(content -> Arrays.stream(content.split(" ")))
                        .collect(Collectors.toList())
                        .size();
            }
        }, Encoders.INT());

        long totalLength = documentLengths.javaRDD().reduce((a, b) -> a + b);
        // 计算总文档数
        long totalDocs = news_Filter.count();

        // 计算平均文档长度
        double averageDocumentLengthInCorpus = (double) totalLength / totalDocs;

        JavaRDD<ArticleNeeded> newsRDD = news_Filter.toJavaRDD();
        JavaRDD<String> words = newsRDD.flatMap(article ->
                article.getContents().stream()
                        .flatMap(content -> Arrays.stream(content.split(" ")))
                        .iterator()
        );

        // 计算词频
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        // 收集并广播词频Map
        Map<String, Integer> termFrequencies = wordCounts.collectAsMap();
        Broadcast<Map<String, Integer>> termFrequenciesBroadcast = sparkContext.broadcast(termFrequencies);

        Dataset<ScoreDistanceMap> scoreAndDistance = news_Filter.flatMap(new FlatMapFunction<ArticleNeeded, ScoreDistanceMap>() {
            @Override
            public Iterator<ScoreDistanceMap> call(ArticleNeeded articleNeeded) throws Exception {
                List<ScoreDistanceMap> res = new ArrayList<>();
                List<Query> queries = queriesBroadcast.value();

                // 文档长度
                int currentDocumentLength = articleNeeded.getContents().stream()
                        .flatMap(content -> Arrays.stream(content.split(" ")))
                        .collect(Collectors.toList())
                        .size();

                TextDistanceCalculator distanceCalculator = new TextDistanceCalculator();

                Map<Query,List<Double>> map= new HashMap<>();
                for (Query query : queries) {
                    String title = articleNeeded.getTitle();
                    if (title == null){ title = "";}
                    double distance = distanceCalculator.similarity(title,query.getOriginalQuery());
                    List<Double> numbers = new ArrayList<>();

                    Double dphScoreSum = 0.0;
                    for (String term : query.getQueryTerms()) {
                        // 当前文档中术语的频率
                        long termFrequencyInCurrentDocument = articleNeeded.getContents().stream()
                                .flatMap(content -> Arrays.stream(content.split(" "))) // 分割每个字符串并扁平化
                                .filter(word -> word.equalsIgnoreCase(term)) // 忽略大小写进行比较
                                .count();
                        // 语料库中术语的总频率
                        if(termFrequencyInCurrentDocument == 0){
                            continue;
                        }
                        int totalTermFrequencyInCorpus = termFrequenciesBroadcast.value().getOrDefault(term, 0);

                        // 计算DPH得分
                        double dphScore = DPHScorer.getDPHScore(
                                (short) termFrequencyInCurrentDocument,
                                totalTermFrequencyInCorpus,
                                currentDocumentLength,
                                averageDocumentLengthInCorpus,
                                totalDocs
                        );

                        if(Double.isNaN(dphScore)) {dphScore = 0.0;}
                        dphScoreSum += dphScore;

                    }
                    numbers.add((double) dphScoreSum / query.getQueryTerms().size());
                    numbers.add(distance);
                    map.put(query,numbers);
                }
                boolean anyNonZero = map.values().stream() // 将values集合转换为Stream
                        .anyMatch(list -> !list.isEmpty() && !list.get(0).equals(0.0)); // 使用anyMatch检查是否存在第一个元素不是0.0的列表

                if(anyNonZero == true){res.add(new ScoreDistanceMap(articleNeeded.getId(),articleNeeded.getTitle(),articleNeeded.getArticle(),map));}

                return res.iterator();
            }
        },Encoders.bean(ScoreDistanceMap.class));
        return scoreAndDistance;
    }

}
