package uk.ac.gla.dcs.bigdata.studentstructures;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import java.io.Serializable;
import java.util.List;

/**
 * A simple structure of Dataset, only include id, title nad content.
 * For easily computing, we rebuild the structure of contents using List rather than json.
 * @author Ruixian Zhang
 *
 */
public class ArticleNeeded implements Serializable{
    // Declaring private member variables
    private String id; // Unique article identifier
    private String title; // Article title
    private List<String> contents; // The contents of the article body which subtype is paragraph and limited to 5 items
    private NewsArticle article; // Reference to the original NewsArticle object

    // Default constructor
    public ArticleNeeded() {

    }

    // Parameterized constructor
    public ArticleNeeded(String id, String title, List<String> contents) {
        super(); // Calls the constructor of the superclass (Object class)
        this.id = id; // Sets the article ID
        this.title = title; // Sets the article title
        this.contents = contents; // Sets the article contents
    }

    // Getter for the article object
    public NewsArticle getArticle() {
        return article;
    }

    // Setter for the article object
    public void setArticle(NewsArticle article) {
        this.article = article;
    }

    // Getter for the article ID
    public String getId() {
        return id;
    }

    // Setter for the article ID
    public void setId(String id) {
        this.id = id;
    }

    // Getter for the article title
    public String getTitle() {
        return title;
    }

    // Setter for the article title
    public void setTitle(String title) {
        this.title = title;
    }

    // Getter for the article contents
    public List<String> getContents() {
        return contents;
    }

    // Setter for the article contents
    public void setContents(List<String> contents) {
        this.contents = contents;
    }
}