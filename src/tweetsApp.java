import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class tweetsApp {

    public static void main(String[] args) throws Exception {

        Path input1 = new Path("resources/700Tweets.txt");
        Path output1 = new Path("hdfs://localhost:50071/out-step1/");
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

        Path output2 = new Path("hdfs://localhost:50071/out-step2/");
        Configuration conf2 = new Configuration();
        ControlledJob job2 = new ControlledJob(conf2);
        job2.getJob().setJarByClass(L2CalculationPerTweet.class);
        job2.getJob().setMapperClass(L2CalculationPerTweet.Mapper2.class);
        job2.getJob().setReducerClass(L2CalculationPerTweet.Reducer2.class);
        job2.getJob().setMapOutputKeyClass(Text.class);
        job2.getJob().setMapOutputValueClass(Text.class);
        job2.getJob().setOutputKeyClass(Text.class);
        job2.getJob().setOutputValueClass(Text.class);
        job2.getJob().setInputFormatClass(tweetsTextInputFormat.class);
        tweetsTextInputFormat.addInputPath(job2.getJob(),output1);
        FileOutputFormat.setOutputPath(job2.getJob(),output2);

        Path output3 = new Path("hdfs://localhost:50071/out-step3/");
        Configuration conf3 = new Configuration();
        ControlledJob job3 = new ControlledJob(conf3);
        job3.getJob().setJarByClass(createPairsFromWords.class);
        job3.getJob().setMapperClass(createPairsFromWords.Mapper3.class);
        job3.getJob().setReducerClass(createPairsFromWords.Reducer3.class);
        job3.getJob().setMapOutputKeyClass(Text.class);
        job3.getJob().setMapOutputValueClass(Text.class);
        job3.getJob().setOutputKeyClass(Text.class);
        job3.getJob().setOutputValueClass(Text.class);
        job3.getJob().setInputFormatClass(tweetsTextInputFormat.class);
        tweetsTextInputFormat.addInputPath(job3.getJob(),output2);
        FileOutputFormat.setOutputPath(job3.getJob(),output3);

        Path output4 = new Path("hdfs://localhost:50071/out-step4/");
        Configuration conf4 = new Configuration();
        ControlledJob job4 = new ControlledJob(conf4);
        job4.getJob().setJarByClass(cosineSimilarity.class);
        job4.getJob().setMapperClass(cosineSimilarity.Mapper4.class);
        job4.getJob().setReducerClass(cosineSimilarity.Reducer4.class);
        job4.getJob().setMapOutputKeyClass(Text.class);
        job4.getJob().setMapOutputValueClass(Text.class);
        job4.getJob().setOutputKeyClass(Text.class);
        job4.getJob().setOutputValueClass(Text.class);
        job4.getJob().setInputFormatClass(tweetsTextInputFormat.class);
        tweetsTextInputFormat.addInputPath(job4.getJob(),output3);
        FileOutputFormat.setOutputPath(job4.getJob(),output4);

        Path output5 = new Path("output");
        Configuration conf5 = new Configuration();
        if(args.length!=1)
            System.out.println("Wrong Number of arguments");
        else
            conf5.setInt("inputN", Integer.parseInt(args[0]));
        ControlledJob job5 = new ControlledJob(conf5);
        job5.getJob().setJarByClass(getNsimilars.class);
        job5.getJob().setMapperClass(getNsimilars.Mapper5.class);
        job5.getJob().setReducerClass(getNsimilars.Reducer5.class);
        job5.getJob().setMapOutputKeyClass(Text.class);
        job5.getJob().setMapOutputValueClass(Text.class);
        job5.getJob().setOutputKeyClass(Text.class);
        job5.getJob().setOutputValueClass(Text.class);
        job5.getJob().setInputFormatClass(tweetsTextInputFormat.class);
        tweetsTextInputFormat.addInputPath(job5.getJob(),output4);
        FileOutputFormat.setOutputPath(job5.getJob(),output5);


        job2.addDependingJob(job1);
        job3.addDependingJob(job2);
        job4.addDependingJob(job3);
        job5.addDependingJob(job4);

        if(job1.getJob().waitForCompletion(true)) {
            System.out.println("phase 1: job complete successfully");
            Counter someCount = job1.getJob().getCounters().findCounter(wordsTfIdf.Mapper1.COUNTERS.TWEETS_NUM);
            job2.getJob().getConfiguration().setLong("tweets_num",someCount.getValue());
            if(job2.getJob().waitForCompletion(true)) {
                System.out.println("phase 2: job complete successfully");
                if(job3.getJob().waitForCompletion(true)){
                    System.out.println("phase 3: job complete successfully");
                    if(job4.getJob().waitForCompletion(true)){
                        System.out.println("phase 4: job complete successfully");
                       if(job5.getJob().waitForCompletion(true)){
                            System.out.println("phase 5: job complete successfully");
                            /* if(job6.getJob().waitForCompletion(true)){
                                System.out.println("phase 6: job complete successfully");
                            }else
                                System.out.println("phase 6: job complete Unsuccessfully");*/
                        }else
                            System.out.println("phase 5: job complete Unsuccessfully");
                    }else
                        System.out.println("phase 4: job complete Unsuccessfully");
                }else
                    System.out.println("phase 3: job complete Unsuccessfully");
            }else
                System.out.println("phase 2: job complete Unsuccessfully");
        }else
            System.out.println("phase 1: job complete Unsuccessfully");
    }
}