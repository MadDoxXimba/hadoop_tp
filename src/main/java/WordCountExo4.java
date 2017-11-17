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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import java.util.Locale;
/*Exercice 4*/
/*


*/

public class WordCountExo4 {
    
    private static enum COUNTRY {
        USA, FR, EGY, AF, AL, DZ, AS, AD, AO, AI, AQ, AG, AR, AM, AW, AU, AT, AZ, BS, BH, BD, BB, BY, BE, BZ, BJ, BM, BT, BO, BA, BW, BR, IO, CN, JPN, CH
    }
    
    /*Modification du mapper pour écrire que les mots commencant par "m"*/
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String line = value.toString();
           
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()){

                word.set(tokenizer.nextToken());
                //modification sur le mapper
                
                for (COUNTRY country : COUNTRY.values()) {
         
                    if (word.toString().equals(country.toString())) {
                    
                        //Compatage d'un occurence
                        
                        context.getCounter("COUNTRY", country.toString()).increment(1);
                        context.write(word, one);
                    }
                }
                
                
                //si modification dans le reducer décommenter ici
                //context.write(word, one);
            }
        }
    }
    
    
    /*Les modifications */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            //mettre en commentaire le if  si c'est le mapper qui fait le boulot
            //if (key.toString().substring(0,1).equals("m")) {
                
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }

                context.write(key, new IntWritable(sum));
                
           // }
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        //Paramètres des jobs
        
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

        //Exo 3
        //il faut attendre que le job soit paramétrer avant d'utiliser le counter
        job.waitForCompletion(true);
        Counters all_counters = job.getCounters();
        Counter counterM = all_counters.findCounter(COUNTRY.USA);
        //ICI on fait un print des top 10 pays
        System.out.println("COUNTRY.USA: Ceci est un compteur custom: "+ counterM.getValue());
    }
}
