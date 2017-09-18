package jwikipedia;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WikipediaRanking {

  static List<String> langs = Arrays.asList(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy"
  );


  //Init SparkConf
  public static SparkConf conf  = null;
  public static JavaSparkContext sc = null;
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  static JavaRDD<WikipediaArticle> wikiRdd = null;

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  public static int occurrencesOfLang(String lang, JavaRDD<WikipediaArticle> rdd){
      return 0;
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  public static List<Tuple2<String, Integer>> rankLangs(List<String> langs , JavaRDD<WikipediaArticle> rdd){
      return null;
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  public static JavaRDD<Tuple2<String, Iterable<WikipediaArticle>>> makeIndex(List<String> langs, JavaRDD<WikipediaArticle> rdd){
      return null;
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  public static List<Tuple2<String, Integer>>  rankLangsUsingIndex(JavaRDD<Tuple2<String, Iterable<WikipediaArticle>>> index){
          return null;
    }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  public static List<Tuple2<String, Integer>>  rankLangsReduceByKey( List<String> langs, JavaRDD<WikipediaArticle> rdd ){
      return null;
  }

  public static void main(String[] args ) {

    /* Languages ranked according to (1) */
    List<Tuple2<String, Integer>> langsRanked = rankLangs(langs, wikiRdd);

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    JavaRDD<Tuple2<String, Iterable<WikipediaArticle>>> index = makeIndex(langs, wikiRdd);

    /* Languages ranked according to (2), using the inverted index */
    List<Tuple2<String, Integer>> langsRanked2 = rankLangsUsingIndex(index);

    /* Languages ranked according to (3) */
    List<Tuple2<String, Integer>>  langsRanked3 = rankLangsReduceByKey(langs, wikiRdd);


    sc.stop();
  }

}
