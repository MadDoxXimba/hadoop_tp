import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;  
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  




public class CompositeKey implements WritableComparable<CompositeKey> {
        public String country;
        public String time;
        public float total;
        
        public CompositeKey() {
    	}
    	
		public CompositeKey(String country, String time) {
    		super();
    		this.set(country, time);

    	}
    
    	public CompositeKey(String country, String time, float total) {
    		super();
    		this.set(country, time, total);

    	}
    	
        public void set(String country, String time, float total) {
            this.country = (country == null) ? "" : country;
            this.country = (time == null) ? "" : time;
            this.total = total;
        }
        
        public void set(String country, String time) {
            this.country = (country == null) ? "" : country;
            this.country = (time == null) ? "" : time;
        }
        

    	public void write(DataOutput out) throws IOException {
            out.writeUTF(country);
            out.writeUTF(time);
            out.writeFloat(total);
    	}
    
    
    	public void readFields(DataInput in) throws IOException {
            this.country = in.readUTF();
            this.time = in.readUTF();
            this.total = in.readFloat();

    	}
    
    	public int compareTo(CompositeKey o) {
            int countrycmp = country.toLowerCase().compareTo(o.country.toLowerCase());
                if (countrycmp != 0) {
                    return countrycmp;
                } else {
                    int timecmp = time.toLowerCase().compareTo(o.time.toLowerCase());
                    if (timecmp != 0) {
                    	return timecmp;
                    } else {
                    	return Float.compare(total, o.total);
                    }
                }
    	}
        
    }