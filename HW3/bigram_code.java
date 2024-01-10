import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.ArrayList;

public class WordCount {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text twoWords = new Text();
    private Text docID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] wholeChunk = value.toString().split("\t", 2);
      String contentChunk = wholeChunk[1].toLowerCase();

      contentChunk = contentChunk.replaceAll("\\s+", " ");
      contentChunk = contentChunk.replaceAll("[^a-z\\s]", " ");

      docID.set(wholeChunk[0]);
      StringTokenizer itr = new StringTokenizer(contentChunk);
      String word1 = itr.nextToken();
      while (itr.hasMoreTokens()) {
        String word2 = itr.nextToken();
        twoWords.set(word1 + " " + word2);
        context.write(twoWords, docID);
        word1 = word2;
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private Text stringResult = new Text();
    ArrayList<Text> gfg = new ArrayList<Text>() {
      {
        add(new Text("computer science"));
        add(new Text("information retrieval"));
        add(new Text("power politics"));
        add(new Text("los angeles"));
        add(new Text("bruce willis"));
      }
    };

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashMap<String, Integer> wordAndCounts = new HashMap<>();
      for (Text value : values) {
        String docID = value.toString();
        Integer data = wordAndCounts.getOrDefault(docID, 0) + 1;

        wordAndCounts.put(docID, data);
      }
      String finalString = new String("");
      for (String x : wordAndCounts.keySet()) {

        finalString = finalString + x + ":";
        finalString = finalString + String.valueOf(wordAndCounts.get(x)) + "\t";
      }
      stringResult.set(finalString.substring(0, finalString.length() - 1));
      System.out.println(gfg.contains(key));
      if(gfg.contains(key))
      {
        context.write(key, stringResult);
      }


    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}