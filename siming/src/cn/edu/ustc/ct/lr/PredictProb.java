package cn.edu.ustc.ct.lr;
import java.io.File;
import java.io.IOException;  

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.filecache.DistributedCache;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.FloatWritable;
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

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.InvalidInputDataException;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;
import de.bwaldvogel.liblinear.Train;

/*
 * 10-21
 * 建立lr模型预测结果
 * 输出为每个mzid及其按照转化概率的排名
 */
  
public class PredictProb extends Configured implements Tool{ 

       
    public static class MyMapper extends Mapper<Text,Text,FloatWritable,Text>{  
    	 
    	
    	Model model = new Model();
    	
        public void map(Text key, Text value, Context context)  
            throws IOException,InterruptedException{ 
        	String[] tok = value.toString().split(" ");
        	Feature[] instance = new Feature[tok.length];
        	int i = 0;
            for(String s : tok){
            	String[] strs = s.split(":");
            	FeatureNode f = new FeatureNode(Integer.parseInt(strs[0]), Double.parseDouble(strs[1]));
            	instance[i] = f;
            	i++;
            }
    		double[] prob = {0.0,0.0};
    		double prediction = Linear.predictProbability(model, instance, prob);
    		int[] labels = model.getLabels();
    		if(labels[0] == 0)
                context.write(new FloatWritable((float)prob[0]), key); 
    		if(labels[1] == 0)
                context.write(new FloatWritable((float)prob[1]), key); 
        }  
          

    
    public void setup(Context context){  
        try {  
            Path [] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());  
            if(null != cacheFiles  && cacheFiles.length > 0){   
                File f = new File(cacheFiles[0].toString());
				try {
				    Configuration conf = context.getConfiguration();
				    String strC  = conf.get("LRc"); 
					SolverType solver = SolverType.L1R_LR; // -s 6
					double C = Double.parseDouble(strC);    // cost of constraints violation
					double eps = 0.01; // stopping criteria
					Parameter parameter = new Parameter(solver, C, eps);
					Problem problem = Train.readProblem(f, 0);	
					model = Linear.train(problem, parameter);
				} catch (InvalidInputDataException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}        		  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    } 
    }  
    
    public static class MyReduce extends Reducer<FloatWritable,Text,Text,LongWritable>{
    	
    	private static long linenum = 1;
        
    	public void reduce(FloatWritable key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
        	for(Text t : values){
        		    context.write(t, new LongWritable(linenum));
        		linenum++;
        	}           
        }   	
    }
    
    public int run(String[] args) throws Exception { 
        if (args.length < 4) { 
	        System.err.println("Usage: main <trainset>  <input> <output> <lr.c>");
	        System.exit(-1);
	    }
        Configuration conf = getConf();
        conf.setStrings("LRc", args[3]);
        Job job = new Job(conf,"PredictProb");  
        job.setJarByClass(PredictProb.class);  
          
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        Path in = new Path(args[1]);  
        Path out = new Path(args[2]);  
          
        conf = job.getConfiguration();  
        FileInputFormat.setInputPaths(job, in); 
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(out);
        fs.close();
        FileOutputFormat.setOutputPath(job, out);  
          
        job.setMapperClass(MyMapper.class); 
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(1);  
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class); 
        job.getConfiguration()  
        .set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");  
        job.setOutputFormatClass(TextOutputFormat.class);  
        System.exit(job.waitForCompletion(true)?0:1);          
        return 0;  
    }  
      
    public static void main(String args[]) throws Exception{  
        int res = ToolRunner.run(new Configuration(), new PredictProb(), args);  
        System.exit(res);  
    }  
      
}  

