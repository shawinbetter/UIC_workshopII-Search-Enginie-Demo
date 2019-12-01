package wordcount;

import org.apache.log4j.PropertyConfigurator;

import wordcount.Task2.Reducer1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task3 {
    // part1------------------------------------------------------------------------
    public static class Mapper_Part1 extends Mapper<LongWritable, Text, Text, Text> {
        String File_name = ""; 
        int all = 0; 
        static Text one = new Text("1");
        String word;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String str = ((FileSplit) inputSplit).getPath().toString();
            String regEx="[^a-zA-Z]";
            File_name = str.substring(str.lastIndexOf("/") + 1);
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll(regEx," "));
            while (itr.hasMoreTokens()) {
                word = File_name;
                word += " ";
                word += itr.nextToken(); 
                all++;
                context.write(new Text(word), one);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            
            String str = "";
            str += all;
            context.write(new Text(File_name + " " + "!"), new Text(str));
            
        }
    }

    public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {
        float all = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int index = key.toString().indexOf(" ");
            
            if (key.toString().substring(index + 1, index + 2).equals("!")) {
                for (Text val : values) {
                    
                    all = Integer.parseInt(val.toString());
                }
                
                return;
            }
            float sum = 0; 
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
           
            float tmp = sum / all;
            String value = "";
            value += tmp; 
            
            String p[] = key.toString().split(" ");
            String key_to = "";
            key_to += p[1];
            key_to += " ";
            key_to += p[0];
            context.write(new Text(key_to), new Text(value));
        }
    }

    public static class Reduce_Part1 extends Reducer<Text, Text, Text, Text> {
    	//*private Text info =new Text();*/
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	/*String fileList=new String();*/
            for (Text val : values) {
                context.write(key, val);
            }
            /*int splitIndex=key.toString().indexOf(" ");
    		info.set(key.toString().substring(splitIndex+1)+" "+fileList);
    		key.set(key.toString().substring(0,splitIndex+1));
    		//result.set(sum);
    		context.write(key, info);*/
        }
    }

    public static class MyPartitoner extends Partitioner<Text, Text> {
       
        public int getPartition(Text key, Text value, int numPartitions) {
            
            
            String ip1 = key.toString();
            ip1 = ip1.substring(0, ip1.indexOf(" "));
            Text p1 = new Text(ip1);
            return Math.abs((p1.hashCode() * 127) % numPartitions);
        }
    }

    // part2-----------------------------------------------------
    public static class Mapper_Part2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().replaceAll("    ", " ");
            
            int index = val.indexOf(" ");
            String s1 = val.substring(0, index); 
            String s2 = val.substring(index + 1); 
           
            s2 += " ";
            s2 += "1"; 
            context.write(new Text(s1), new Text(s2));
        }
    }

    public static class combiner extends Reducer<Text, Text, Text, Text> {
    	private Text info =new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String fileList=new String();
            for (Text val : values) {
                context.write(key, val);
            }
            int splitIndex=key.toString().indexOf(" ");
    		info.set(key.toString().substring(splitIndex+1)+" "+fileList);
    		key.set(key.toString().substring(0,splitIndex+1));
    		//result.set(sum);
    		context.write(key, info);
        }
    }
    
    public static class Reduce_Part2 extends Reducer<Text, Text, Text, Text> {
        int file_count;
        private Text result=new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String fileList =new String();
    		for (Text value : values) {
    			fileList += value.toString()+" ";
    		}
            file_count = context.getNumReduceTasks(); 
            float sum = 0;
            List<String> vals = new ArrayList<String>();
            for (Text str : values) {
                int index = str.toString().lastIndexOf(" ");
                sum += Integer.parseInt(str.toString().substring(index + 1)); 
                vals.add(str.toString().substring(0, index)); 
            }
            double tmp = Math.log10(file_count * 1.0 / (sum * 1.0));
                                                                     // = IDF
            for (int j = 0; j < vals.size(); j++) {
                String val = vals.get(j);
                String end = val.substring(val.lastIndexOf(" "));
                float f_end = Float.parseFloat(end); 
                val += " ";
                val += f_end * tmp; // tf-idf
                result.set(fileList);
                context.write(key, new Text(val));
            }
        }
        /*private Text result=new Text();
    	public void reduce(Text key, Iterable<Text> values, Context context)
    	throws IOException, InterruptedException {
    		String fileList =new String();
    		for (Text value : values) {
    			fileList += value.toString()+" ";
    		}
    		result.set(fileList);
    		context.write(key, result);
    	}*/
    }

	/*public static void main(String[] args) throws Exception {
		
		Path temp = new Path("hdfs://qheng1:9000/data/ws_output/output_1"); 
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf1 = new Configuration();
		FileSystem hdfs = FileSystem.get(conf1);
		FileStatus p[] = hdfs.listStatus(new Path("config/log4j.properties"));
		Job job1 = new Job(conf1, "Task3_1");
		job1.setJarByClass(Task3.class);
		job1.setMapperClass(Mapper_1.class);
		job1.setCombinerClass(Combiner_1.class); 
		job1.setReducerClass(Reduce_1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(p.length);
		job1.setPartitionerClass(Parti.class); 
		FileInputFormat.addInputPath(job1, new Path("hdfs://qheng1:9000/11/"));
		FileOutputFormat.setOutputPath(job1, temp);
		job1.waitForCompletion(true);
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Task3_2");
		job2.setJarByClass(Task3.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(Mapper_2.class);
		job2.setReducerClass(Reduce_2.class);
		job2.setNumReduceTasks(p.length);
		FileInputFormat.setInputPaths(job2, temp);
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://qheng1:9000/data/ws_output/output_2"));
	}*/
    public static void main(String[] args) throws Exception {
    	long start,end;
    	start=System.currentTimeMillis(); 
		PropertyConfigurator.configure("config/log4j.properties");

        // part1----------------------------------------------------
        Configuration conf1 = new Configuration();
        
        FileSystem hdfs = FileSystem.get(conf1);
        
       
        Job job1 = Job.getInstance(conf1, "Task3_1");
        job1.setJarByClass(Task3.class);
        job1.setMapperClass(Mapper_Part1.class);
        job1.setCombinerClass(Combiner_Part1.class);
        job1.setReducerClass(Reduce_Part1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        job1.setPartitionerClass(MyPartitoner.class); 
        FileInputFormat.addInputPath(job1, new Path("hdfs://qheng1:9000/11/")); 
    	FileOutputFormat.setOutputPath(job1, new Path("hdfs://qheng1:9000/data/ws_output/output_2")); 
        //FileInputFormat.addInputPath(job1, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        //job1.waitForCompletion(true);
    	end=System.currentTimeMillis();
        System.out.println("start time:"+start+"; end time:" +end+"; Run Time:"+(end-start)+"(ms)");
    	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        System.exit(job1.waitForCompletion(true) ? 0 : 1); 
        // part2----------------------------------------
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Task3_2");
        job2.setJarByClass(Task3.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Mapper_Part2.class);
        job2.setCombinerClass(combiner.class);
        job2.setReducerClass(Reduce_Part2.class);
        //job2.setNumReduceTasks(p.length);
	FileInputFormat.addInputPath(job2, new Path("hdfs://qheng1:9000/11/")); 
	FileOutputFormat.setOutputPath(job2, new Path("hdfs://qheng1:9000/data/ws_output/output_3")); 
	//job2.waitForCompletion(true);
	//FileInputFormat.addInputPath(job, new Path(args[0])); 
	//FileOutputFormat.setOutputPath(job, new Path(args[1])); 
	/*end=System.currentTimeMillis();
    System.out.println("start time:"+start+"; end time:" +end+"; Run Time:"+(end-start)+"(ms)");
	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");*/
	System.exit(job2.waitForCompletion(true) ? 0 : 1); 
	}

}
