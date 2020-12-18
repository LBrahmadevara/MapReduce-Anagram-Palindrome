import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.lang.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public class Spark_Anagram {

    // private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sprkConfing = new SparkConf().setAppName("Spark Anagram");
        JavaSparkContext sparkContext = new JavaSparkContext(sprkConfing);
        // to read data in recursive directories
        sparkContext.hadoopConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        // reading data from the input files
        JavaRDD<String> lines = sparkContext.textFile(inputPath, 1);
        // looping through each line
        JavaPairRDD<String, String> RDDresult = lines.flatMapToPair(
                new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(String line) throws Exception {
                        // Get all the words in the line 
                        String[] words = StringUtils.split(line.trim().toLowerCase());
                        if (words == null) {
                            return Collections.EMPTY_LIST.iterator();
                        }
                        List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
                        for (String word : words) {
                            // here removing special characters
                            word = word.trim().replaceAll("[^a-zA-Z0-9\\s]+", "");
                            if (word.length() < 2) {
                                continue;
                            }
                            char[] wordChars = word.toCharArray();
                            Arrays.sort(wordChars);
                            String sortedWord = new String(wordChars);
                            results.add(new Tuple2<String, String>(sortedWord, word));
                        }
                        return results.iterator();
                    }
                });

        JavaPairRDD<String, Iterable<String>> filteredData = 
            RDDresult.distinct().groupByKey().filter((Function<Tuple2<String, Iterable<String>>, Boolean>) v1 -> {
            // checking if there is an anagram for a particular word
            if(Iterables.size(v1._2) <= 1)
                return false;
            return true;
        });
        filteredData.saveAsTextFile(outputPath);
        System.out.println("Job Done!!!");
        sparkContext.close();
        System.exit(0);
    }
}