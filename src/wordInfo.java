import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class wordInfo implements Writable {

    private long tweetID;
    private double tf;
    private double idf;
    private double tfIdf;


    public wordInfo(long tweetID, double tf, double idf){
        this.tweetID = tweetID;
        this.tf = tf;
        this.idf=idf;
    }

    public wordInfo(wordInfo other){
        this.tweetID = other.getTweetID();
        this.tf = other.getTweetTf();
        this.idf = other.getIdf();
        this.tfIdf = other.getTfidf();
    }

    public void setIdf(Double idf) {this.idf = idf;}

    public void setTf(Double tf) {this.tf = tf;}

    public void setTfIdf(Double tfIdf) {this.tfIdf = tfIdf;}

    public void setTweetID(long tweetID){this.tweetID = tweetID;}

    public long getTweetID(){return this.tweetID;}

    public Double getIdf(){return  this.idf;}

    public Double getTweetTf(){return this.tf;}

    public Double getTfidf(){return  this.tfIdf;}

    @Override
    public void readFields(DataInput in) throws IOException {
        tweetID = in.readLong();
        tf = in.readLong();
        idf = in.readDouble();
        tfIdf = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tweetID);
        out.writeDouble(tf);
        out.writeDouble(idf);
        out.writeDouble(tfIdf);
    }

    @Override
    public String toString(){
        String ans = tweetID +"\t" + String.valueOf(tf)+"\t" + String.valueOf(idf);
        return ans;
    }
}
