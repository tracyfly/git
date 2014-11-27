package cn.edu.ustc.ct.lr;

import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.filecache.DistributedCache;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;

/*
 * 10-22提价满足条件的结果
 * 用于将排名中满足要求的结果提交
 * 一般用于测试单模型
 */
  
public class TopID extends Configured implements Tool{ 
	
    public static class MyMapper extends Mapper<Text,Text,LongWritable,Text>{  
    	    	
        public void map(Text key, Text value, Context context)  
            throws IOException,InterruptedException{
        	    Long va = Long.parseLong(value.toString());
                context.write(new LongWritable(va), key);  
        }            
    }  
    
    public static class MyReduce extends Reducer<LongWritable,Text,Text,Text>{
    	
    	private static long linenum = 0;
    	public  int tag; 
        
    	public void reduce(LongWritable key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
        	for(Text t : values){
        		if(linenum < tag)
        		    context.write(t, new Text(""));
        		linenum++;
        		if(linenum > tag)
        			linenum = tag + 1;
        	}           
        }
    	
        protected void setup(Context context)
                throws IOException, InterruptedException {
			      Configuration conf = context.getConfiguration();
			      String num  = conf.get("predictnum"); 
			      tag = Integer.parseInt(num);
        }
    }
    
    public int run(String[] args) throws Exception { 
        if (args.length < 3) { 
	        System.err.println("Usage: main <input> <output> <outputnumber>");
	        System.exit(-1);
	    }
        Configuration conf = getConf();
        conf.setStrings("predictnum", args[2]);
        Job job = new Job(conf,"TopID");  
        job.setJarByClass(TopID.class);  
          
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        Path in = new Path(args[0]);  
        Path out = new Path(args[1]);                 
        
        FileInputFormat.setInputPaths(job, in);
        
        conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(out);
        fs.close();
        FileOutputFormat.setOutputPath(job, out);  
          
        job.setMapperClass(MyMapper.class); 
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(1);  
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class); 
        job.getConfiguration()  
        .set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");  
        job.setOutputFormatClass(TextOutputFormat.class);  
        System.exit(job.waitForCompletion(true)?0:1);          
        return 0;  
    }  
      
    public static void main(String args[]) throws Exception{  
        int res = ToolRunner.run(new Configuration(), new TopID(), args);  
        System.exit(res);  
    }  
      
}  

