
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * cora wu
 * for jar: Lucene-core，lucene-analyzers-common，lucene-queryparser
 * index at the ubuntu
 */
public class lucenefirst {
    public static Version luceneVersion = Version.LATEST;
    //index first
    public static void createIndex(){
        IndexWriter writer = null;
        try{
            //set up Directory
            Directory directory = FSDirectory.open(Paths.get("/home/uic/Desktop/test"));
            //IndexWriter
            IndexWriterConfig iwConfig = new IndexWriterConfig( new StandardAnalyzer());
            writer = new IndexWriter(directory, iwConfig);
            Document document = null;//initial document
            //initial field
            File f = new File("/home/uic/Desktop/text new");//the file from the ubuntu
            for (File file:f.listFiles()){
                    document = new Document();
                    document.add(new StringField("path", f.getName(),Field.Store.YES));
                    System.out.println(file.getName());
                    document.add(new StringField("name", file.getName(),Field.Store.YES));
                    InputStream stream = Files.newInputStream(Paths.get(file.toString()));
                    document.add(new TextField("content", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
                    writer.addDocument(document);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            //close the writer
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException
    {
        createIndex();
    }
}
