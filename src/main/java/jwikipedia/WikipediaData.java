package jwikipedia;

import java.io.File;
import java.net.URL;

class WikipediaData {

  private String filePath() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource("wikipedia.dat");
    if (resource == null)
      throw new Exception("Please download the dataset as explained in the assignment instructions");
    return new File(resource.getFile()).getPath();
  }

  private WikipediaArticle parse(String line) {
    String subs = "</title><text>";
    int i = line.indexOf(subs);
    String title = line.substring(14, i);
    String text  = line.substring(i + subs.length(), line.length()-16);
    return new WikipediaArticle(title, text);
  }
}
