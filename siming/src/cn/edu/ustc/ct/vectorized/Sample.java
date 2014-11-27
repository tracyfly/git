package cn.edu.ustc.ct.vectorized;

import java.io.BufferedReader;  
import java.io.FileReader;  
import java.io.IOException;  
import java.util.HashMap;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.filecache.DistributedCache;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
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
 * 10-26
 * 抽样得到训练集  
 * 正样本存入distributedcache,全用
 * 负样本下采样，取前N个
 */
  
public class Sample extends Configured implements Tool{  
  
	
    public static class MyMapper extends Mapper<Text,Text,Text,Text>{  
    	
        private HashMap<String,String> joinData = new HashMap<String,String>();  
        public void map(Text key, Text value, Context context)  
            throws IOException,InterruptedException{  
            String joinValue = joinData.get(key.toString());
            if(null != joinValue){  
                context.write(key, new Text("1\t" + value.toString()));  
            } 
            else{
            	context.write(key, new Text("0\t" + value.toString()));  
            }
        }  
          
        public void setup(Context context){  
            try {  
                Path [] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());  
                if(null != cacheFiles  && cacheFiles.length > 0){  
                	for(int i = 0; i < cacheFiles.length; i++){
                		String line;  
                			String []tokens;  
                			BufferedReader br = new BufferedReader(new FileReader(cacheFiles[i].toString()));  
                			try{  
                				while((line = br.readLine()) != null){  
                					tokens = line.split("\\^");  
                					joinData.put(tokens[0], "1");  
                              
                				}  
                			}finally{  
                				br.close();  
                			}  
                	}
                }  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
    }  
    
    public static class MyReduce extends Reducer<Text,Text,Text,Text>{
    	private int samplenum;
    	private static long linenum = 0;
        
    	public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
        	for(Text va : values){
        		String[] strs = va.toString().split("\t");
        		if(strs[0].equals("1"))
        		    context.write(va, new Text(""));
        		else if(linenum < 1389 * samplenum){       			
        			context.write(va, new Text(""));
        		    linenum++;
        		}
        		else
        			linenum = 1389 * samplenum + 2;
        	}           
        }  
    	
    	  @Override
          protected void setup(Context context)
                  throws IOException, InterruptedException {
			      Configuration conf = context.getConfiguration();
			      String num  = conf.get("num"); 
			      samplenum = Integer.parseInt(num);
          }
    }
    
    public int run(String[] args) throws Exception {  
        if (args.length < 5) { 
	        System.err.println("Usage: main <transform1> <transform2> <input> <output> <count>");
	        System.exit(-1);
	    }
        Configuration conf = getConf();
        conf.setStrings("num",args[4]); 
        Job job = new Job(conf,"Sample");  
        job.setJarByClass(Sample.class);  
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());  
        DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());
        Path in = new Path(args[2]);  
        Path out = new Path(args[3]);  
        
        conf = job.getConfiguration();
        FileInputFormat.setInputPaths(job, in);  
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(out);
        fs.close();
        FileOutputFormat.setOutputPath(job, out);  
          
        job.setMapperClass(MyMapper.class); 
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(1);  
        job.setInputFormatClass(KeyValueTextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.getConfiguration()  
            .set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");  
        //在新API 中不再是key.value.separator.in.input.line，你可以在源码KeyValueLineRecordReader.java中看见。   
        System.exit(job.waitForCompletion(true)?0:1);  
          
        return 0;  
    }  
      
    public static void main(String args[]) throws Exception{  
        int res = ToolRunner.run(new Configuration(), new Sample(), args);  
        System.exit(res);  
    }  
      
}  