import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import java.io.IOException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class Spark_Palindrome {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static boolean isPalindrome(String str){
        if (str.length() > 1)
            return str.equals(new StringBuilder(str).reverse().toString());
        return false;
    }

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("Spark Palindrome")
        .set("spark.shuffle.service.enabled", "false")
        .set("spark.dynamicAllocation.enabled", "false");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // to read data in recursive directories
        sparkContext.hadoopConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        JavaRDD<String> lines = sparkContext.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s.trim().toLowerCase())).iterator());
        // removing all symbols
        JavaRDD<String> filteredWords = words.flatMap(s -> Arrays.asList(s.trim().replaceAll("[^a-zA-Z0-9\\s]+", "")).iterator());
        // count of occurence of each word
        JavaPairRDD<String, Integer> ones = filteredWords.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.filter(s->Spark_Palindrome.isPalindrome(s._1)).sortByKey(true).reduceByKey((i1, i2) -> 1);
        List<Tuple2<String, Integer>> output = counts.collect();
        counts.saveAsTextFile(outputPath);
        for (Tuple2<?,?> tuple : output) {
            if(tuple._1().toString().length()>1) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }
        System.out.println("Job Done!!!");

    }
}
