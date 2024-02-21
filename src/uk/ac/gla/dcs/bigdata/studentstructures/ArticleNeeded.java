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
    private String id; // unique article identifier
    private String title; // article title
    private List<String> contents; // the contents of the article body which subtype is paragraph and limited 5 items

    private NewsArticle article;

    public ArticleNeeded(){

    }

    public ArticleNeeded(String id, String title, String author, List<String> contents) {
        super();
        this.id = id;
        this.title = title;
        this.contents = contents;
    }

    public NewsArticle getArticle() {
        return article;
    }

    public void setArticle(NewsArticle article) {
        this.article = article;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getContents() {
        return contents;
    }

    public void setContents(List<String> contents) {
        this.contents = contents;
    }
}
