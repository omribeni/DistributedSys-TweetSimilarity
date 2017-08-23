import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class tweetKey implements WritableComparable<tweetKey> {

    private String created_at;
    private long id;


    public tweetKey (String created_at, long id){
        this.created_at = created_at;
        this.id = id;
    }
    public String getCreated_at() {
        return created_at;
    }

    public long getId() {
        return id;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        created_at = in.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeChars(created_at);
    }

    @Override
    public int compareTo(tweetKey other) {
        if(this.created_at.equals(other.created_at) && this.id== other.id) return 0;
        else return 1;
    }

    @Override
    public String toString() {
        return created_at;
    }
}
