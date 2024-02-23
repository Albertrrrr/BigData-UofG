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

        // Collect queries into a list
        List<Query> queriesList = queries.collectAsList();

        // Broadcast the list of queries
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        Broadcast<List<Query>> queriesBroadcast = sparkContext.broadcast(queriesList);

        // Compute a Dataset of document lengths
        Dataset<Integer> documentLengths = news_Filter.map(new MapFunction<ArticleNeeded, Integer>() {
            @Override
            public Integer call(ArticleNeeded article) throws Exception {
                return article.getContents().stream()
                        .flatMap(content -> Arrays.stream(content.split(" ")))
                        .collect(Collectors.toList())
                        .size();
            }
        }, Encoders.INT());

        // Calculate the total length of all documents and the total number of documents
        long totalLength = documentLengths.javaRDD().reduce((a, b) -> a + b);
        long totalDocs = news_Filter.count();

        // Calculate the average document length in the corpus
        double averageDocumentLengthInCorpus = (double) totalLength / totalDocs;

        // Convert news_Filter to an RDD and flatMap to words
        JavaRDD<ArticleNeeded> newsRDD = news_Filter.toJavaRDD();
        JavaRDD<String> words = newsRDD.flatMap(article ->
                article.getContents().stream()
                        .flatMap(content -> Arrays.stream(content.split(" ")))
                        .iterator()
        );

        // Count word frequencies
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        // Collect and broadcast the term frequencies map
        Map<String, Integer> termFrequencies = wordCounts.collectAsMap();
        Broadcast<Map<String, Integer>> termFrequenciesBroadcast = sparkContext.broadcast(termFrequencies);

        // Compute scores and distances
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
                        // Frequency of terms in the current document
                        long termFrequencyInCurrentDocument = articleNeeded.getContents().stream()
                                // Split and flatten each string
                                .flatMap(content -> Arrays.stream(content.split(" ")))
                                // Ignore case for comparison
                                .filter(word -> word.equalsIgnoreCase(term))
                                .count();

                        // Total frequency of terms in the corpus
                        if(termFrequencyInCurrentDocument == 0){
                            continue;
                        }
                        int totalTermFrequencyInCorpus = termFrequenciesBroadcast.value().getOrDefault(term, 0);

                        // Calculate DPH score
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
                // Convert the values collection to a Stream.
                boolean anyNonZero = map.values().stream()

                        // Use anyMatch to check if there is a list whose first element is not 0.0.
                        .anyMatch(list -> !list.isEmpty() && !list.get(0).equals(0.0));

                if(anyNonZero == true){res.add(new ScoreDistanceMap(articleNeeded.getId(),articleNeeded.getTitle(),articleNeeded.getArticle(),map));}

                return res.iterator();
            }
        },Encoders.bean(ScoreDistanceMap.class));
        return scoreAndDistance;
    }

}
