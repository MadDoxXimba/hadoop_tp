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
import java.util.*;
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
        
        //ascendant
        public static <K, V extends Comparable<? super V>> Map<K, V> 
            sortByValue(Map<K, V> map) {
            List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return (o1.getValue()).compareTo( o2.getValue() );
                }
            });
    
            Map<K, V> result = new LinkedHashMap<K, V>();
            for (Map.Entry<K, V> entry : list) {
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
        //descendant
        public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
            Comparator<K> valueComparator =  new Comparator<K>() {
                public int compare(K k1, K k2) {
                    int compare = map.get(k2).compareTo(map.get(k1));
                    if (compare == 0) return 1;
                    else return compare;
                }
            };
            Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
            sortedByValues.putAll(map);
            return sortedByValues;
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            
        //a faire: sort sur hashmap
        

            Map<String , Integer>  sortedMap = new HashMap<String , Integer>();
            sortedMap = sortByValues(map);
            
            
            //que les top 10
            int topN = 10;
            int currentIndex = 0;
            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                
                if (currentIndex < topN) {
                    String key_sorted = entry.getKey();
                    Integer value_sorted = entry.getValue();
                    context.write(new Text(key_sorted), new IntWritable(value_sorted));
                    currentIndex++;
                }

            }
    
    
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
