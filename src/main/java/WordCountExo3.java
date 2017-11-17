import java.io.IOException;
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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*Exercice 3*/
public class WordCountExo3 {
    
    
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            //convertion de la 1ere ligne du fichier en string
            String line = value.toString();
            //convertion de chaque mot du string en token et suppression des mots usuels (ex: a, by, for, etc...)
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()){
                //selection du prochain mot sur le string
                word.set(tokenizer.nextToken());
                //distribution des tâches sur différents serveurs du cluster
                context.write(word, one);
            }
        }
    }
    
    

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //opération maths entre chaque couple créer par le mapper
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //ecriture du fichier résultat sur le cluster
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // -Dinput=/home/ubuntu/workspace/tp1/formation-bigdata/dataset/wordcount/hamlet.txt -Doutput=/home/ubuntu/workspace/tp1/formation-bigdata/dataset/wordcount/output.txt
        String inputPath = args[0].replace("-Dinput=","");
        String outputPath = args[1].replace("-Doutput=","");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        

        
    }
}
