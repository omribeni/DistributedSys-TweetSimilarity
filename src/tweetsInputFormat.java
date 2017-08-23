import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class tweetsInputFormat extends FileInputFormat<tweetKey, tweetValue> {


    @Override
    public RecordReader<tweetKey, tweetValue> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new tweetsRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        System.out.println(file.toString());
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}
