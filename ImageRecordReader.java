import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Treats key as the file's path and value as the (whole) file's contents.
 */
public class ImageRecordReader extends RecordReader<Text, Text> {

  private Text mKey;
  private Text mValue;
  private boolean mWasUsed;

  public ImageRecordReader() {
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    mWasUsed = false;

    FileSplit split = (FileSplit) genericSplit;
    Path path = split.getPath();
    mKey = new Text(path.toString());

    int fileLength = (int) split.getLength();
    byte[] buffer = new byte[fileLength];
    FileSystem fs = path.getFileSystem(context.getConfiguration());
    FSDataInputStream inputStream = fs.open(path);
    IOUtils.readFully(inputStream, buffer, 0, fileLength);
    mValue = new Text(buffer);
  }

  public boolean nextKeyValue() {
    if (!mWasUsed) {
      mWasUsed = true;
      return true;
    }
    return false;
  }

  public Text getCurrentKey() {
    return mKey;
  }

  public Text getCurrentValue() {
    return mValue;
  }

  public float getProgress() {
    return 1.0f;
  }

  public void close() {
  }

}
