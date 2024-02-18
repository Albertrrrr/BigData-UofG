package uk.ac.gla.dcs.bigdata.studentfunctions;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
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
        if (jsonMapper == null) jsonMapper = new ObjectMapper();

        //Parsing Raw Data with Jackson
        NewsArticle originalArticle = jsonMapper.readValue(value.mkString(), NewsArticle.class);

        // Create a new NewsArticle object containing only the required fields
        ArticleNeeded filteredArticle = new ArticleNeeded();
        filteredArticle.setId(originalArticle.getId());
        filteredArticle.setTitle(originalArticle.getTitle());
        List<String> filteredContentStrings = originalArticle.getContents().stream()
                .filter(item -> item != null && "paragraph".equals(item.getSubtype())) //First filter out items that are not null and have a subtype of paragraph
                .map(ContentItem::getContent) // Converts each ContentItem to string.
                .collect(Collectors.toList()); // Result into List<String>


        // Limited 5 items in every Content
        if (filteredContentStrings.size() > 5) {
            filteredContentStrings = filteredContentStrings.subList(0, 5);
        }

        // Check items and set contents.
        // System.out.println("Contents Count: " + Integer.toString(filteredContentStrings.size()));

        // processing Text
        TextPreProcessor text = new TextPreProcessor();
        List<String> processedText = new ArrayList<>();

        for(String term: filteredContentStrings){
            List<String> processedTerms = text.process(term);
            //System.out.println(processedTerms);
            String processedRes = String.join(" ",processedTerms);
            processedText.add(processedRes);
        }
        filteredArticle.setContents(processedText);

        return filteredArticle;
    }
}
