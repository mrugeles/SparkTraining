package jwikipedia;

public class WikipediaArticle{
    private String title;
    private String text;

    public WikipediaArticle(String title, String text) {
        this.title = title;
        this.text = text;
    }
    /**
     * @return Whether the text of this article mentions `lang` or not
     * @param lang Language to look for (e.g. "Scala")
     */
    public boolean mentionsLanguage(String lang){
        return text.contains(lang);
    }

    public String getTitle() {
        return title;
    }

    public String getText() {
        return text;
    }
}
