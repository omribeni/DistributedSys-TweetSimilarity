import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class wordsTfIdf {

    private static Log log = LogFactory.getLog(wordsTfIdf.class);

    public static class Mapper1 extends Mapper<tweetKey, tweetValue, Text, Text> {
        public enum COUNTERS {
            TWEETS_NUM,
            ALL_WORDS_NUM,
        }

        private static Map<String,Integer> stopWords = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException,InterruptedException {

            /*try {
                Scanner sc = new Scanner(new File("resources/stopWords.txt"));
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    line=  line.replace("\n","");
                    stopWords.put(line, 1);
                }
                sc.close();

            }catch (Exception ex) {
                ex.printStackTrace();
            }*/
            AWSCredentials credentials = new PropertiesCredentials(awsTweetsApp.class.getResourceAsStream("/AWSCredentials.properties"));

            AmazonS3 s3 = new AmazonS3Client(credentials);
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);

            System.out.print("Downloading stopwords file from S3... ");
            S3Object object = s3.getObject(new GetObjectRequest("shaked/resource", "stopwords.txt"));
            System.out.println("Done.");

            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            String word;
            while((word = br.readLine()) != null){
                if(!this.stopWords.containsKey(word))
                    this.stopWords.put(word, 1);
            }
            System.out.println("Phase 1: Finished Setup");
        }


        /**
         * input: [tweetKey , tweetValue]
         * output (to reducer): [word, <tweetID, tf>]
         *
         * @param key - tweetKey
         * @param value - tweetValue
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(tweetKey key, tweetValue value, Context context) throws IOException, InterruptedException {

            Map<String,Integer> wordsCountPerTweet = new HashMap<>();
            List<String> words = new ArrayList<>();
            int maxWordCount = 1;
            context.getCounter(COUNTERS.TWEETS_NUM).increment(1);       //num of tweets counter
            String text = value.getText();

            // count word occurrences per tweet
            // iterate and store every word in text in hash table. if the word already exist just increment the word's counter
            StringTokenizer itr1 = new StringTokenizer(text);
            while (itr1.hasMoreTokens()) {
                String curr = itr1.nextToken();
                if (!stopWords.containsKey(curr)) {
                    if (wordsCountPerTweet.containsKey(curr)) {
                        int currWordValue = wordsCountPerTweet.get(curr);
                        int newValue = currWordValue + 1;
                        wordsCountPerTweet.replace(curr, newValue);
                        if (newValue > maxWordCount)
                            maxWordCount = newValue;
                    } else {
                        wordsCountPerTweet.put(curr, 1);
                        words.add(curr);
                        context.getCounter(COUNTERS.ALL_WORDS_NUM).increment(1);
                    }
                }
            }

            //calculate tf
            for (String currWord : words) {
                Text word = new Text();
                Text id_tf = new Text();
                Double tf = 0.5 + (0.5 * (wordsCountPerTweet.get(currWord) / maxWordCount));
                word.set(currWord);
                id_tf.set(key.getId()+"@!@"+ tf + "@!@"+ key.toString() + "#!#" + value.toString());
                context.write(word, id_tf);
            }
        }

    }


    public static class Reducer1 extends Reducer<Text,Text,Text, Text> {


        /**
         * input:[word, iterable<tweetID, tf>]
         * output: [word, <tweetID, tf, idf_denominator>]
         *
         * @param key - word
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            Vector<Text> tempValues = new Vector<>();

            //calculate idf_denominator
            for (Text value : values) {
                Text tempValue = new Text(value);
                sum++;
                tempValues.add(tempValue);
            }

            for (Text value : tempValues){
                String[] value_id_tf = value.toString().split("@!@");
                String tweetID = value_id_tf[0];
                String tf = value_id_tf[1];
                String idf_denominator = String.valueOf(1/sum);
                //log.info("word: "+ key+ "   tweetId: "+ tweetID+ "  tf: "+ tf+ "   idf: "+ idf_denominator);
                context.write(key, new Text(tweetID+"@!@"+tf+"@!@"+idf_denominator+"@!@"+value_id_tf[2]));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Path input1 = new Path(args[0]);
        Path output1 = new Path(args[1]);
        Configuration conf1 = new Configuration();
        ControlledJob job1 = new ControlledJob(conf1);
        job1.getJob().setJarByClass(wordsTfIdf.class);
        job1.getJob().setMapperClass(wordsTfIdf.Mapper1.class);
        job1.getJob().setReducerClass(wordsTfIdf.Reducer1.class);
        job1.getJob().setMapOutputKeyClass(Text.class);
        job1.getJob().setMapOutputValueClass(Text.class);
        job1.getJob().setOutputKeyClass(Text.class);
        job1.getJob().setOutputValueClass(Text.class);
        job1.getJob().setInputFormatClass(tweetsInputFormat.class);
        tweetsInputFormat.addInputPath(job1.getJob(), input1);
        FileOutputFormat.setOutputPath(job1.getJob(), output1);

        if(job1.getJob().waitForCompletion(true)) {
            System.out.println("phase 1: job complete successfully");
            Counter someCount = job1.getJob().getCounters().findCounter(wordsTfIdf.Mapper1.COUNTERS.TWEETS_NUM);
        }else
            System.out.println("phase 1: job complete Unsuccessfully");
    }

}
