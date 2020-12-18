import org.apache.hadoop.mapred.MapReduceBase;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Hadoop_Anagram {

    public static class HadoopAnagramMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text sorted_text = new Text();
        private Text original_text = new Text();
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter)
            throws IOException {
            StringTokenizer line = new StringTokenizer(value.toString().toLowerCase());
            // lopping through the words in each line
            while (line.hasMoreTokens()) {
                String word = line.nextToken().replaceAll("[^a-zA-Z0-9\\s]+", "");
                // converting words to characters
                char[] wordChars = word.toCharArray();
                Arrays.sort(wordChars);
                // sorting characters and changing it to string
                String sortedKey = new String(wordChars);
                sorted_text.set(sortedKey);
                original_text.set(word);
                outputCollector.collect(sorted_text, original_text);
            }
        }
    }

    public static class HadoopAnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text opKey = new Text();
        private Text opVal = new Text();
        @Override
        public void reduce(Text key, Iterator<Text> val, OutputCollector<Text, Text> results, Reporter reporter)
            throws IOException {
            String strOutput = "\t";
            Integer count = 0;
            Boolean commaFlag = false;
            // using set to eliminate duplicates
            Set<String> anagramsSet = new HashSet<>();
            while (val.hasNext()) {
                anagramsSet.add(val.next().toString());
            }
            if (anagramsSet.size() > 1) {
                // concatinating different anagrams
                for (String temp : anagramsSet) {
                    if (commaFlag) {
                        strOutput += ", ";
                    }
                    strOutput += temp;
                    commaFlag = true;
                }
                opKey.set(key.toString());
                opVal.set(strOutput);
                results.collect(opKey, opVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Hadoop_Anagram.class);
        conf.setJobName("Hadoop Anagram");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(HadoopAnagramMapper.class);
        conf.setReducerClass(HadoopAnagramReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        Job job = Job.getInstance(conf);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}