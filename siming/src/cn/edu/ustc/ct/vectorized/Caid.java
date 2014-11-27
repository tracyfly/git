package cn.edu.ustc.ct.vectorized;
import java.io.IOException;  
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;


/*
 * 11-13
 * 将caid的点击和曝光分开考虑
 */

public class Caid extends Configured implements Tool{  
       
    public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{  
    	 
        public void map(LongWritable key, Text value, Context context)  
            throws IOException,InterruptedException{ 
        	String[] tok = value.toString().split("\\^");
        	String va = tok[2].substring(4) +  "^" + tok[tok.length - 1];
            context.write(new Text(tok[0]), new Text(va));  
        }  
          
    }  
    
    //起到化简作用
    public static class MyCombiner extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{

        	HashMap<String, String> hm = new HashMap<String, String>();      	
        	for(Text t : values)
        		hm.put(t.toString(), "1");
        	for(String s : hm.keySet()){
        		context.write(key, new Text(s));
        	}
        }      
    }
    
    
    public static class MyReduce extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
        	TreeMap<Integer,Double> mymap = new TreeMap<Integer, Double>();  
        	
        	for(Text t : values){
        		String[] str = t.toString().split("\\^");
        		int k = Integer.parseInt(str[0]);  
        		if(str[1].equals("IMP")){
            		if(mymap.containsKey(k) == false)
            			mymap.put(k, 1.0);
        		}
        		else{
            		if(mymap.containsKey(k + 1749) == false)
            			mymap.put(k + 1749, 1.0);
        		}
        			
        	}
        	String strs = "";
        	for(Integer ind : mymap.keySet()){
        		strs += String.valueOf(ind) + ":" + String.valueOf(mymap.get(ind)) + " ";
        	}
            context.write(key, new Text(strs));
        }      
    }
    
    public int run(String[] args) throws Exception {  
        if (args.length < 2) { 
	        System.err.println("Usage: main <input> <output>");
	        System.exit(-1);
	    }
        Configuration conf = getConf();  
        Job job = new Job(conf,"Caid");  
        job.setJarByClass(Caid.class);  
            
        Path in = new Path(args[0]);  
        Path out = new Path(args[1]);  
        
        conf = job.getConfiguration();
        FileInputFormat.setInputPaths(job, in);  
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(out);
        fs.close();
        FileOutputFormat.setOutputPath(job, out);  
        
        job.setNumReduceTasks(18);
        job.setMapperClass(MyMapper.class); 
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReduce.class);  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        System.exit(job.waitForCompletion(true)?0:1);          
        return 0;  
    }  
      
    public static void main(String args[]) throws Exception{  
        int res = ToolRunner.run(new Configuration(), new Caid(), args);  
        System.exit(res);  
    }  
      
}  


