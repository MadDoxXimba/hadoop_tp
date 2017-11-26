import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;


import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;


/*Exercice 4*/
/*


*/


public class WordCountExo4 {
    
    //on définit une clé composite

    
    
    

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
            // for full word count on csv
            
            String line = value.toString();
            String[] s = line.split("\t", -1);
            
            if (s[1].toString().equals("") || s[7].toString().equals("")) {
                word.set("UNDEFINED.COUNTRY");
                context.write(word, one);
            } else {
                 //word.set("Date:"+s[1].toString() + ", Pays:" + s[7].toString());
                 word.set(s[7].toString());
                 context.write(word, one);
            }

        }
    }
    
    

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        Map<String , Integer > map = new HashMap<String , Integer>();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                
                map.put(key.toString() , sum); 
                
                //context.write(key, new IntWritable(sum));
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            
        //a faire: sort sur hashmap

        //     Map<String , Integer>  sortedMap = new HashMap<String , Integer>();
        //     sortedMap = sortMap(map);
            
        //   for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
        //         String key_sorted = entry.getKey();
        //         Integer value_sorted = entry.getValue();
        //         context.write(new Text(key_sorted), new IntWritable(value_sorted));
        //     }
    
    
        }
        

    }
    
    public static void main(String[] args) throws Exception {
        
        //Paramètres des jobs
        
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "WordCount");
        
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        // job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String inputPath = args[0].replace("-Dinput=","");
        String outputPath = args[1].replace("-Doutput=","");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        
    }
}
