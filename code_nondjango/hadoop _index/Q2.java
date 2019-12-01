package wordcount;
import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;
public class Q2 {
        public static class TokenizerMapper 
        		extends Mapper<Object, Text, Text, Text>{
        			private Text keyInfo = new Text();
        			private Text valueInfo=new Text();
        			int i=0;
        			public void map(Object key, Text value, Context context) throws
        			                IOException, InterruptedException {
        				//split = (FileSplit) context.getInputSplit();
        				String regEx="[^a-zA-Z]";
        				String fileName=((FileSplit)context.getInputSplit()).getPath().getName();
        				i++;
        				StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll(regEx," "));
        				while (itr.hasMoreTokens()) {
        					//word.set(itr.nextToken());
        					keyInfo.set(itr.nextToken()+" "+fileName);
        					valueInfo.set(String.valueOf(i));
        					context.write(keyInfo, valueInfo);
        			}
        		}
        }
        public static class IntSumcombine 
        extends Reducer<Text,Text,Text,Text>{
        	//private IntWritable result = new IntWritable();
        	private Text info =new Text();
        	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException {
        		String fileList=new String();
        		/*for(Text val:values) {
        			fileList+=val.toString()+" ";
        		}*/
        		int splitIndex=key.toString().indexOf(" ");
        		info.set(key.toString().substring(splitIndex+1)+" "+fileList);
        		key.set(key.toString().substring(0,splitIndex+1));
        		//result.set(sum);
        		context.write(key, info);
        	}
        }
        public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        	private Text result=new Text();
        	public void reduce(Text key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
        		String fileList =new String();
        		for (Text value : values) {
        			fileList += value.toString()+" ";
        		}
        		result.set(fileList);
        		context.write(key, result);
        	}
        }
        public static void main(String[] args)throws Exception {
        	long start,end;
        	start=System.currentTimeMillis(); 
        	PropertyConfigurator.configure("config/log4j.properties");
        	Configuration conf=new Configuration();
        	Job job=Job.getInstance(conf,"word count");
        	job.setJarByClass(wordcount.class);
        	job.setMapperClass(TokenizerMapper.class);
        	job.setCombinerClass(IntSumcombine.class);
        	job.setReducerClass(InvertedIndexReducer.class);
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(Text.class);
        	end=System.currentTimeMillis();
        	FileInputFormat.addInputPath(job, new Path("hdfs://qheng1:9000/11/"));
    		FileOutputFormat.setOutputPath(job, new Path("hdfs://qheng1:9000/data/ws_output/get_line"));
            end=System.currentTimeMillis();
            System.out.println("start time:"+start+"; end time:" +end+"; Run Time:"+(end-start)+"(ms)");
        	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        	System.exit(job.waitForCompletion(true)?0:1);
        }
}
