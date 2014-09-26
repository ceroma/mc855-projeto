import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An {@link InputFormat} for grayscale images represented in plain text files.
 * Images are broken down into pixels. Keys are the pixels' positions in the form "row:column" and
 * values are the pixels' grayscale value.
 */
public class ImageInputFormat extends FileInputFormat<Text, IntWritable> {

  @Override
  public RecordReader<Text, IntWritable> createRecordReader(
      InputSplit split,
      TaskAttemptContext context) {
    return new ImageRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return true;
  }

}
