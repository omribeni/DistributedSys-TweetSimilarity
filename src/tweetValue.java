import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class tweetValue implements Writable {

    private String userName;
    private String text;
    private boolean favorited;
    private boolean retweeted;

    public tweetValue (String userName, String text, boolean favorited, boolean retweeted){
        this.userName = userName;
        this.text = text;
        this.favorited = favorited;
        this.retweeted = retweeted;
    }

    public String getUserName() {
        return userName;
    }

    public String getText() {
        return text;
    }

    public boolean getFavorited() {
        return favorited;
    }

    public boolean getRetweeted() {
        return retweeted;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userName = in.toString();
        text = in.toString();
        favorited = in.readBoolean();
        retweeted = in.readBoolean();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(userName);
        out.writeChars(text);
        out.writeBoolean(favorited);
        out.writeBoolean(retweeted);
    }

    @Override
    public String toString() {
        return favorited +"#!#"+ retweeted + "#!#" + text;
    }
}
