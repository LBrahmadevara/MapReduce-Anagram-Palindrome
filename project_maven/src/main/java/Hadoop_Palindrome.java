import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;


public class Hadoop_Palindrome {
    public static boolean isPalindrome(String str){
        if (str.length() > 1)
            return str.equals(new StringBuilder(str).reverse().toString());
        return false;
    }

    public static class Palin_Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
            // lopping through the words in each line
            while (itr.hasMoreTokens()) {
                // removing special characters
                word.set(itr.nextToken().replaceAll("[^a-zA-Z0-9\\s]+", ""));
                if(word.getLength()>0)
                    if(Hadoop_Palindrome.isPalindrome(word.toString()))
                        // sending key, value pairs
                        context.write(word, new IntWritable(1));
            }
        }
    }

    public static class Palin_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        // refered mapreduce example
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // returning the list of palindromes
            context.write(key, new IntWritable(1));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Palindrome Check");
        job.setJarByClass(Hadoop_Palindrome.class);
        job.setMapperClass(Palin_Mapper.class);
        job.setCombinerClass(Palin_Reducer.class);
        job.setReducerClass(Palin_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}