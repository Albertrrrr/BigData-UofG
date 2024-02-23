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

//Allowing instances of this class to be serialized, a requirement for distributed computing scenarios often encountered in Spark applications.
public class CheckJson implements Serializable {
    // valid
    // toFile takes SparkSession, Dataset of Query, and Dataset of ArticleNeeded as parameters
    public void toFile (SparkSession spark, Dataset<Query> queries, Dataset<ArticleNeeded> news_Filter ){

        // Transform the news_Filter dataset to extract strings using flatMap
        Dataset<String> contents = news_Filter.flatMap(

                // Define a FlatMapFunction that processes each ArticleNeeded object
                (FlatMapFunction<ArticleNeeded, String>) article -> {

                    // Get the contents of the article, assuming it's a List<String>
                    List<String> articlesContents = article.getContents();

                    // Return an iterator over the contents if not null, otherwise return an iterator over an empty list
                    return articlesContents != null ? articlesContents.iterator() : new ArrayList<String>().iterator();
                },
                Encoders.STRING() // Specify the Encoder for the output data type!
        );

        // Other processing, such as saving of content
        // Try-catch block to handle IOExceptions during file writing
        try {
            List<String> collectedContents = contents.collectAsList();

            // Write the collected contents to a file named "allContents2.txt" in the project's root directory
            Files.write(Paths.get("./allContents2.txt"), collectedContents);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
