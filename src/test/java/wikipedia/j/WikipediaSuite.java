package wikipedia.j;

import jwikipedia.WikipediaArticle;
import jwikipedia.WikipediaRanking;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.scalatest.BeforeAndAfterAll;
import org.scalatest.ConfigMap;
import org.scalatest.FunSuite;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static jwikipedia.WikipediaRanking.*;

/**
 * Created by mrugeles on 17/09/2017.
 */
public class WikipediaSuite{

    public boolean initializeWikipediaRanking() {
        try {
            String[] init = {"Hello", "World"};
            WikipediaRanking.main(init);
            return true;
        } catch(Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * Creates a truncated string representation of a list, adding ", ...)" if there
     * are too many elements to show
     * @param l The list to preview
     * @param n The number of elements to cut it at
     * @return A preview of the list, containing at most n elements.
     */
    public String previewList(List<String> l, int n)
    {
        List<String> lines = null;
        StringBuilder sb = new StringBuilder();
        if (l.size() <= n) lines = l;
        else lines =  l.subList(0,n);
        for (String line: lines) {
            sb.append(line);
            
        }
        return sb.toString();
    }

    /**
     * Asserts that all the elements in a given list and an expected list are the same,
     * regardless of order. For a prettier output, given and expected should be sorted
     * with the same ordering.
     * @param given The actual list
     * @param expected The expected list
     * @tparam A Type of the list elements
     */
    public void assertSameElements(List<String> given, List<String> expected) {

        List<String> _given = null;
        List<String> _expected = null;

        Collections.copy(_given, given);
        Collections.copy(_expected, expected);

        _given.removeAll(expected);
        _expected.removeAll(given);

        boolean noUnexpectedElements = _given.size() == 0;
        boolean noMissingElements = _expected.size() == 0;

        //String noMatchString = "|Expected: "+previewList(expected, 10)+"  |Actual:   "+ previewList(given, 10);

        Assert.assertTrue(noUnexpectedElements);
        Assert.assertTrue(noMissingElements);

    }

    //test("'occurrencesOfLang' should work for (specific) RDD with one element") {
     @Test
    public void testOcurrences(){
       List<WikipediaArticle> list =  Arrays.asList(new WikipediaArticle("title", "Java Jakarta"));
       JavaRDD<WikipediaArticle> rdd = sc.parallelize(list);
       boolean res = (occurrencesOfLang("Java", rdd) == 1);
       Assert.assertTrue(res);
    }


    //test("'rankLangs' should work for RDD with two elements") {
    @Test
    public void testRankLangs(){
        List<String> langs = Arrays.asList("Scala", "Java");
        JavaRDD<WikipediaArticle> rdd = sc.parallelize(Arrays.asList(new WikipediaArticle("1", "Scala is great"), new WikipediaArticle("2", "Java is OK, but Scala is cooler")));
        List<Tuple2<String, Integer>> ranked = rankLangs(langs, rdd);
        Assert.assertEquals(ranked.get(0)._1(), "Scala");
    }

    @Test
    public void testMakeIndex(){
        List<String> langs = Arrays.asList("Scala", "Java");
        List<WikipediaArticle> articles = Arrays.asList(
                new WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
                new WikipediaArticle("2","Scala and Java run on the JVM"),
                new WikipediaArticle("3","Scala is not purely functional")
        );
        JavaRDD<WikipediaArticle> rdd = sc.parallelize(articles);
        JavaRDD<Tuple2<String, Iterable<WikipediaArticle>>> index = makeIndex(langs, rdd);
        Assert.assertEquals(index.count(), 2);
    }

    @Test
    public void testRankLangsUsingIndex(){
        List<String> langs = Arrays.asList("Scala", "Java");
        List<WikipediaArticle> articles = Arrays.asList(
                new WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
                new WikipediaArticle("2","Scala and Java run on the JVM"),
                new WikipediaArticle("3","Scala is not purely functional")
        );

        JavaRDD<WikipediaArticle> rdd = sc.parallelize(articles);
        JavaRDD<Tuple2<String, Iterable<WikipediaArticle>>> index = makeIndex(langs, rdd);
        List<Tuple2<String, Integer>> ranked = rankLangsUsingIndex(index);
        Assert.assertEquals(ranked.get(0)._1(), "Scala");
    }

    @Test
    public void testRankLangsReduceByKey(){
        List<String> langs = Arrays.asList("Scala", "Java", "Groovy", "Haskell", "Erlang");
        List<WikipediaArticle> articles = Arrays.asList(
                new WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
                new WikipediaArticle("2","Scala and Java run on the JVM"),
                new WikipediaArticle("3","Scala is not purely functional"),
                new WikipediaArticle("4","The cool kids like Haskell more than Java"),
                new WikipediaArticle("5","Java is for enterprise developers")
        );

        JavaRDD<WikipediaArticle> rdd = sc.parallelize(articles);
        List<Tuple2<String, Integer>> ranked = rankLangsReduceByKey(langs, rdd);
        Assert.assertEquals(ranked.get(0)._1(), "Java");
    }
}
