import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Simple FileInputFormat that reads the whole file, instead of splitting it.
 */
public class ImageInputFormat extends FileInputFormat<Text, Text> {

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new ImageRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}
