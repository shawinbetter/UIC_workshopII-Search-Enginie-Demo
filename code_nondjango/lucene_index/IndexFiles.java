//cora wu group13
import java.io.IOException;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


public class IndexFiles {
//search the file which has changed into lucenefirst at the desktop
	public static String indexSearch(String keywords){
        String res = "";
        DirectoryReader reader = null;
        try{
             Directory directory = FSDirectory.open(Paths.get("/home/uic/Desktop/text new"));
             reader = DirectoryReader.open(directory);
             IndexSearcher searcher =  new IndexSearcher(reader);

             QueryParser parser = new QueryParser("content",new StandardAnalyzer());
             Query query = parser.parse(keywords);
             TopDocs tds = searcher.search(query, 20);
             ScoreDoc[] sds = tds.scoreDocs;
             int cou=0;
             for(ScoreDoc sd:sds){
                 cou++;
                 Document d = searcher.doc(sd.doc);
                 res+= " {"+d.get("name")+"}"+"\n";
             }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }
	
	//map process
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{	

	    private Text result = new Text();
	    //private String pattern1 = "[^a-zA-Z0-9]";
	    private String pattern = "[\\pP|\\pS|\\d+]";
	    private String inf;

	    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	
	    	String o = value.toString().toLowerCase();
	    	o = o.replaceAll(pattern, "");
	    	StringTokenizer itr = new StringTokenizer(o);

	    while(itr.hasMoreTokens()) {
	    	String s1 = itr.nextToken();
	    	inf = s1 +  "__:" + indexSearch(s1);
	    	inf = inf.replaceAll("\\s+", "");
	    	result.set(inf);
	    	context.write(result, new Text(" "));
	    	}
		}
	}
	

	//combiner process
	public static class Combiner extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();
		private Text file = new Text();
    
	    public void reduce(Text key, Iterable<Text> values, Context context)throws
	    IOException, InterruptedException {
	    	String[] string = key.toString().split("\\,");
	    	String word = string[0];
	    	String words = string[0];
	    	for(Text value: values) {
	    		word += " " + value.toString() + " ";
	    	}
	    	file.set(word);
	    	result.set(words.toString());
	    	context.write(result, file);
	    }
	}
//reduce process
	public static class Reduce
	    extends Reducer<Text,Text,Text,Text> {
	    Text result = new Text();
	    
	    public void reduce(Text key, Iterable<Text> values, Context
	            context ) throws IOException, InterruptedException {
	            String words= new String();
	           
	            for (Text value : values) {
	                     //words = words + "{" + value.toString() + " " + "}";
	                     words = words;
	            }
	            result.set(words);
	            context.write(key, result);
	    }
	}

	
//the main method to execute the method
	public static void main(String[] args) throws Exception { 
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "result index");
		job.setJarByClass(IndexFiles.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    
		FileInputFormat.addInputPath(job, new Path("hdfs://0305666:9000/data/text new")); 
		FileOutputFormat.setOutputPath(job, new Path("hdfs://0305666:9000/data/lucenewhole")); 

		//FileInputFormat.addInputPath(job, new Path(args[0])); 
		//FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
		}
}