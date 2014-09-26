import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Scanner;

/**
 * Treats key as a pixel's position in the form "row:column" and value as the pixel's grayscale
 * value.
 */
public class ImageRecordReader extends RecordReader<Text, IntWritable> {

  private Text mKey;
  private IntWritable mValue;

  private int mCurrRow;
  private int mCurrCol;
  private int mNumRows;
  private int mNumCols;

  private Scanner mScanner;

  public ImageRecordReader() {
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Path path = split.getPath();

    int fileLength = (int) split.getLength();
    byte[] buffer = new byte[fileLength];
    FileSystem fs = path.getFileSystem(context.getConfiguration());
    FSDataInputStream inputStream = fs.open(path);
    IOUtils.readFully(inputStream, buffer, 0, fileLength);

    mScanner = new Scanner((new Text(buffer)).toString());
    mNumRows = mScanner.nextInt();
    mNumCols = mScanner.nextInt();
    mCurrRow = mCurrCol = 0;
  }

  public boolean nextKeyValue() {
    if (mCurrRow >= mNumRows) {
      return false;
    }

    mValue = new IntWritable(mScanner.nextInt());
    mKey = new Text(Integer.toString(mCurrRow) + ":" + Integer.toString(mCurrCol));

    mCurrCol++;
    if (mCurrCol >= mNumCols) {
      mCurrRow++;
      mCurrCol = 0;
    }

    return true;
  }

  public Text getCurrentKey() {
    return mKey;
  }

  public IntWritable getCurrentValue() {
    return mValue;
  }

  public float getProgress() {
    return ((float) ((mCurrRow + 1) * (mCurrCol + 1))) / ((float) (mNumRows * mNumCols));
  }

  public void close() {
    mScanner.close();
  }

}
