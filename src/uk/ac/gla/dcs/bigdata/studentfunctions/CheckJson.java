package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleNeeded;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoreDistanceMap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CheckJson implements Serializable {
    // valid
    public void toFile (SparkSession spark, Dataset<Query> queries, Dataset<ArticleNeeded> news_Filter ){
        Dataset<String> contents = news_Filter.flatMap(
                (FlatMapFunction<ArticleNeeded, String>) article -> {
                    // 这假设 getContents() 返回的是 List<String>
                    List<String> articlesContents = article.getContents();
                    return articlesContents != null ? articlesContents.iterator() : new ArrayList<String>().iterator();
                },
                Encoders.STRING() // 指定 Encoder，这非常重要
        );

        // 其他处理，比如内容的保存等
        try {
            List<String> collectedContents = contents.collectAsList();
            Files.write(Paths.get("./allContents2.txt"), collectedContents);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
