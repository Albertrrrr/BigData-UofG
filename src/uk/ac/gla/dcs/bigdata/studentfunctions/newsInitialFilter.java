package uk.ac.gla.dcs.bigdata.studentfunctions;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleNeeded;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Processing of the original Dataset and generation of compliant data for further computing
 * @author Ruixian Zhang
 *
 */

public class newsInitialFilter implements MapFunction<Row,ArticleNeeded> {

    private static final long serialVersionUID = -4631167868446468097L;

    private transient ObjectMapper jsonMapper;


    @Override
    public ArticleNeeded call(Row value) throws Exception {
        // Check if ObjectMapper is not initialized and initialize it
        if (jsonMapper == null) jsonMapper = new ObjectMapper();

        // Parse the row into a NewsArticle object
        NewsArticle originalArticle = jsonMapper.readValue(value.mkString(), NewsArticle.class);

        // Create a new NewsArticle object containing only the required fields
        ArticleNeeded filteredArticle = new ArticleNeeded();
        filteredArticle.setId(originalArticle.getId());
        filteredArticle.setTitle(originalArticle.getTitle());
        filteredArticle.setArticle(originalArticle);

        // Filter and process the contents of the NewsArticle
        List<String> filteredContentStrings = originalArticle.getContents().stream()
                .filter(item -> item != null && "paragraph".equals(item.getSubtype())) //First filter out items that are not null and have a subtype of paragraph
                .map(ContentItem::getContent) // Converts each ContentItem to string.
                .collect(Collectors.toList()); // Result into List<String>


        // Limit the contents to the first 5 items if there are more than 5
        if (filteredContentStrings.size() > 5) {
            filteredContentStrings = filteredContentStrings.subList(0, 5);
        }

        // Add the title to the list of contents to be processed
        filteredContentStrings.add(filteredArticle.getTitle());

        // Check items and set contents.
        // System.out.println("Contents Count: " + Integer.toString(filteredContentStrings.size()));

        // Initialize TextPreProcessor for further text processing
        TextPreProcessor text = new TextPreProcessor();
        List<String> processedText = new ArrayList<>();

        // Process each term in the filtered content strings
        for(String term: filteredContentStrings){
            List<String> processedTerms = text.process(term);
            String processedRes = String.join(" ",processedTerms);
            processedText.add(processedRes);
        }

        // Set the processed contents in the ArticleNeeded object
        filteredArticle.setContents(processedText);

        // Return the processed ArticleNeeded object
        return filteredArticle;
    }
}
