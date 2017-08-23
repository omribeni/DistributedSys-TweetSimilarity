import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;


public class tweetsRecordReader extends RecordReader<tweetKey,tweetValue> {

    LineRecordReader reader;

    tweetsRecordReader(){
        reader = new LineRecordReader();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }


    @Override
    public tweetKey getCurrentKey() throws IOException, InterruptedException  {
        try {
            JSONObject tweetJson = new JSONObject(reader.getCurrentValue().toString());
            return new tweetKey(tweetJson.getString("created_at"),tweetJson.getLong("id"));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public tweetValue getCurrentValue() throws IOException, InterruptedException {
        try {
            JSONObject tweetJson = new JSONObject(reader.getCurrentValue().toString());
            JSONObject userJson = new JSONObject(tweetJson.getJSONObject("user").toString());
            return new tweetValue(userJson.get("name").toString(),tweetJson.get("text").toString().replace("\n"," ").replace("\t"," ").replace("\r"," "),
                    tweetJson.getBoolean("favorited"),tweetJson.getBoolean("retweeted"));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
