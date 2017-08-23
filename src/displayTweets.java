import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;


public class displayTweets extends tweetsApp {

        public static class Mapper6 extends Mapper<Text,Text,Text,Text> {

            @Override
            public void map(Text id1, Text all_ids, Context context) throws IOException, InterruptedException {
                context.write(id1,all_ids);
            }
        }


        public static class Reducer6 extends Reducer<Text,Text,Text,Text> {

            public void reduce(Text id1, Iterable<Text> all_Ids, Context context) throws IOException, InterruptedException {
                String created_at , favorited , retweeted , text , currId;
                double score;
                int count = 1;
                StringBuilder jsonTweet= new StringBuilder();
                StringBuilder jsonSimilarTweets = new StringBuilder("\nSimilarTweets: \n");
                String[] idsAndScore = all_Ids.iterator().next().toString().split("#");
                try{
                        Path pt=new Path("resources/300tweets.txt");
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                        String line;
                        boolean findID1 = false;
                        line = br.readLine();
                        while (line != null) {
                            JSONObject tweet = new JSONObject(line);
                            created_at = tweet.getString("created_at").toString();
                            favorited = tweet.getString("favorited").toString();
                            retweeted = tweet.getString("retweeted").toString();
                            text = tweet.getString("text").toString().replace("\n",". ");

                            if (!findID1 && tweet.getLong("id") == Long.parseLong(id1.toString())){
                                jsonTweet.append("Tweet id: " + id1.toString() + ",  created_at: " + created_at + ",  favorited: "+ favorited + ", retweeted: "+ retweeted + ",  text: "+ text );
                                findID1 = true;
                            }
                            for(int i=0;i<idsAndScore.length;i++) {
                                String[] idsAndScoreElement = idsAndScore[i].split("@");
                                currId = idsAndScoreElement[0];
                                score = Double.valueOf(idsAndScoreElement[1]);
                                DecimalFormat df = new DecimalFormat("#.###");
                                if (tweet.getLong("id") == Long.parseLong(currId)) {
                                    jsonSimilarTweets.append("     " + count + ". similarity: " + df.format(score) +", id: "+ tweet.getString("id")+  ", text: " + text + "\n");
                                    count++;
                                }
                            }
                            line = br.readLine();
                        }
                        context.write(new Text(jsonTweet.toString()),new Text(jsonSimilarTweets.toString()));
                        fs.close();
                }catch(Exception e){}

            }
        }
}
