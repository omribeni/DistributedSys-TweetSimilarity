import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class getNsimilars {
    private static Log log = LogFactory.getLog(createPairsFromWords.class);

    public static class Mapper5 extends Mapper<Text,Text,Text,Text> {


        @Override
        public void map(Text t1Andt2, Text score, Context context) throws IOException, InterruptedException {

            String[] split = t1Andt2.toString().split("@!@");
            Text t1= new Text(split[0]+"@!@"+split[2]);
            Text t2AndScore = new Text(split[1]+"@!@"+ split[3]+" @!@"+ score.toString());
            Text t2= new Text(split[1]+"@!@"+split[3]);
            Text t1AndScore = new Text(split[0]+"@!@"+split[2]+" @!@"+  score.toString());
            //log.info("t1: "+ t1 +"  t2AndScore: " +t2AndScore);
            //log.info("t2: "+ t2 +"  t1AndScore: " +t1AndScore);
            context.write(t1,t2AndScore);
            context.write(t2,t1AndScore);
        }
    }


    public static class Reducer5 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text t1, Iterable<Text> t2AndScore, Context context) throws IOException, InterruptedException {

            DecimalFormat df = new DecimalFormat("#.####");
            String created_at , favorited , retweeted , text1Text , tweetId, similarId , t2Text;
            String[] t1_id_info = t1.toString().split("@!@");
            String[] info = t1_id_info[1].split("#!#");
            tweetId = t1_id_info[0];
            created_at = info[0];
            favorited = info[1];
            retweeted = info[2];
            text1Text = info[3];

            int n = context.getConfiguration().getInt("inputN",1);
            //log.info("=================================================="+ n + "=========================================================");
            List<Pair<String,Double>> bestMatches = new ArrayList<>(n);
            boolean found;
            int pos,max;

            for(Text secondID : t2AndScore){
                String[] idAndScore = secondID.toString().split("@!@");
                Pair<String,Double> p = new Pair<>(idAndScore[0]+"@!@"+idAndScore[1],Double.parseDouble(idAndScore[2]));
                if(bestMatches.size()==0)
                    bestMatches.add(p);
                else{
                    pos=0;
                    found=false;
                    max=bestMatches.size();
                    while(pos<max && !found){
                        if(p.getValue() <= bestMatches.get(pos).getValue())
                            pos++;
                        else {
                            if(bestMatches.size()<n-1) {
                                bestMatches.add(pos, p);
                                found=true;
                            }
                            else{
                                for (int i = bestMatches.size() - 1; i > pos; i--)
                                    bestMatches.set(i, bestMatches.get(i - 1));
                                bestMatches.set(pos, p);
                                found=true;
                            }
                        }
                    }
                    if(!found && bestMatches.size()<n)
                        bestMatches.add(p);
                }
            }

//            for(Text secondID : t2AndScore){
//                String[] idAndScore = secondID.toString().split("@@@");
//                Pair<String,Double> p = new Pair<>(idAndScore[0]+"@@@"+idAndScore[1],Double.parseDouble(idAndScore[2]));
//                if(bestMatches.size()==0)
//                    bestMatches.add(p);
//                else{
//                    pos=0;
//                    found=false;
//                    max = bestMatches.size();
//                    while(pos<max && !found){
//                        if(p.getValue() < bestMatches.get(pos).getValue())
//                            pos++;
//                        else {
//                            for (int i = bestMatches.size() - 1; i > pos; i--)
//                                bestMatches.set(i, bestMatches.get(i - 1));
//                            bestMatches.set(pos, p);
//                            found=true;
//                            }
//                    }
//                    if(!found && bestMatches.size()<n)
//                        bestMatches.add(p);
//                    }
//            }

            int count = 1;
            String tweet ="Tweet id: " + tweetId.toString() + ",  created_at: " + created_at + ",  favorited: "+ favorited + ", retweeted: "+ retweeted + ",  text: "+ text1Text;
            StringBuilder similarTweets = new StringBuilder("\nSimilarTweets: \n");

            for (Pair similar : bestMatches){
                String[] currSimilar = similar.getKey().toString().split("@!@");
                similarId = currSimilar[0];
                String[] currSimilarText = currSimilar[1].split("#!#");
                t2Text = currSimilarText[3];
                similarTweets.append("        " + count + ". similarity: " + df.format(similar.getValue()) +", id: "+ similarId+  ", text: " + t2Text + "\n");
                count++;
            }

            /*String simList="";
            if(bestMatches.size()<n) {
                for (int i = 0; i < bestMatches.size() - 1; i++)
                    simList += bestMatches.get(i).getID()+ "@@@" + bestMatches.get(i).getScore() + "###";
                simList += bestMatches.get(bestMatches.size() - 1).getID() + "@@@" + bestMatches.get(bestMatches.size() - 1).getScore();
            }else{
                for(int i=0;i<n-1;i++)
                    simList += bestMatches.get(i).getID()+ "@@@" + bestMatches.get(i).getScore() + "###";
                simList += bestMatches.get(bestMatches.size() - 1).getID() + "@@@" + bestMatches.get(bestMatches.size() - 1).getScore();
            }
            context.write(t1,new Text(simList));*/
            context.write(new Text(tweet),new Text(similarTweets.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
        Path output5 = new Path(args[1]);
        Configuration conf5 = new Configuration();
        if(args.length!=3)
            System.out.println("Wrong Number of arguments");
        else
            conf5.setInt("inputN", Integer.parseInt(args[2]));
        ControlledJob job5 = new ControlledJob(conf5);
        job5.getJob().setJarByClass(getNsimilars.class);
        job5.getJob().setMapperClass(getNsimilars.Mapper5.class);
        job5.getJob().setReducerClass(getNsimilars.Reducer5.class);
        job5.getJob().setMapOutputKeyClass(Text.class);
        job5.getJob().setMapOutputValueClass(Text.class);
        job5.getJob().setOutputKeyClass(Text.class);
        job5.getJob().setOutputValueClass(Text.class);
        job5.getJob().setInputFormatClass(tweetsTextInputFormat.class);
        tweetsTextInputFormat.addInputPath(job5.getJob(),new Path(args[0]));
        FileOutputFormat.setOutputPath(job5.getJob(),output5);
    }
}
