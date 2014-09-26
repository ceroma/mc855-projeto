import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ImageFilter {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: java ImageFilter input_file output_file");
      return;
    }

    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "ImageFilter");
    job.setJarByClass(ImageFilter.class);

    job.setInputFormatClass(ImageInputFormat.class);
    job.setMapperClass(ImageFilterMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(ImageFilterReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * An input to this Mapper is a single pixel of the image.
   * This Mapper will then compute how this pixel affects the Sobel filter applied to each one
   * of its 8 neighbour pixels, for both horizontal and vertical components.
   *
   * For instance, given the Sobel operators:
   *
   *             +1  0  -1              +1  +2  +1
   * Horizontal: +2  0  -2    Vertical:  0   0   0
   *             +1  0  -1              -1  -2  -1
   *
   * And the sample matrix below:
   *
   *     ?  ?  ?  ?  ?
   *     ?  a  b  c  ?
   * M:  ?  d  e  f  ?
   *     ?  g  h  i  ?
   *     ?  ?  ?  ?  ?
   *
   * The pixel 'e' would contribute with -1*e to the horizontal component of the Sobel filter
   * applied window centered in 'a', 0 to the one centered in 'b', +1*e to the one centered in 'c',
   * +2*e to the one centered in 'f', and so on.
   *
   *                                      +------------------+
   *                                      | +1*?   0*?  -1*? |   ?     ?
   *                                      |                  |
   *                                      | +2*?   0*a  -2*b |   c     ?
   *                                      |                  |
   * Horizontal operator centered in 'a': | +1*?   0*d  -1*e |   f     ?
   *                                      +------------------+
   *                                           ?     g     h     i     ?
   *
   *                                           ?     ?     ?     ?     ?
   *
   * An output key will then represent the position of a neighbour pixel, plus ":H" for horizontal
   * or ":V" for vertical, and the value will represent the contribution of the current pixel to the
   * weighted sum.
   */
  public static class ImageFilterMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    private static final int[][] SOBEL_HORIZONTAL = {
      {1, 0, -1},
      {2, 0, -2},
      {1, 0, -1}
    };

    private static final int[][] SOBEL_VERTICAL = {
      { 1,  2,  1},
      { 0,  0,  0},
      {-1, -2, -1}
    };

    private static final int[] DELTAS = {-1, 0, 1};

    protected void map(Text key, IntWritable value, Context context)
        throws IOException, InterruptedException {

      String[] indexes = key.toString().split(":");
      int inputRow = Integer.valueOf(indexes[0]);
      int inputCol = Integer.valueOf(indexes[1]);

      int pixelValue = value.get();
      int windowSize = DELTAS.length;
      for (int i = 0; i < windowSize; i++) {
        for (int j = 0; j < windowSize; j++) {
          String outputRow = Integer.toString(inputRow + DELTAS[i]);
          String outputCol = Integer.toString(inputCol + DELTAS[j]);
          int outputSobelVertical = SOBEL_VERTICAL[windowSize - 1 - i][windowSize - 1 - j];
          int outputSobelHorizontal = SOBEL_HORIZONTAL[windowSize - 1 - i][windowSize - 1 - j];
          context.write(
              new Text(outputRow + ":" + outputCol + ":H"),
              new IntWritable(pixelValue * outputSobelHorizontal));
          context.write(
              new Text(outputRow + ":" + outputCol + ":V"),
              new IntWritable(pixelValue * outputSobelVertical));
        }
      }
    }

  }

  /**
   * An input to this Reducer is the list of terms of the expression that computes one of the
   * Sobel components (horizontal or vertical) applied to the pixel whose position is given by the
   * input key.
   *
   * This single reducer will then finalize the computation of the Sobel operator and store the
   * results in memory until all pixels have been processed, after which it will output the
   * filtered image.
   */
  public static class ImageFilterReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
    private int mNumRows;
    private int mNumCols;
    private Map<String, Integer> mOutputMap;

    protected void setup(Context context) {
      mNumRows = mNumCols = 0;
      mOutputMap = new HashMap<String, Integer>();
    }

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
      String[] indexes = key.toString().split(":");
      int row = Integer.valueOf(indexes[0]);
      int col = Integer.valueOf(indexes[1]);
      mNumRows = Math.max(mNumRows, row);
      mNumCols = Math.max(mNumCols, col);

      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      // Resulting pixel is SQRT(Sx^2 + Sy^2)
      sum = sum * sum;
      String mapKey = row + ":" + col;
      if (!mOutputMap.containsKey(mapKey)) {
        mOutputMap.put(mapKey, sum);
      } else {
        sum += mOutputMap.get(mapKey);
        double newPixel = Math.sqrt(sum);
        mOutputMap.put(mapKey, (int) Math.min(Math.max(newPixel, 0), 255));
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Writes filtered image to output
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(mNumRows + " " + mNumCols + '\n');

      for (int j = 0; j < mNumCols; j++) {
        stringBuilder.append(j != mNumCols - 1 ? "0 " : "0\n");
      }

      for (int i = 1; i < mNumRows -1; i++) {
        stringBuilder.append("0");
        for (int j = 1; j < mNumCols - 1; j++) {
          String mapKey = i + ":" + j;
          stringBuilder.append(" " + mOutputMap.get(mapKey));
        }
        stringBuilder.append(" 0\n");
      }

      for (int j = 0; j < mNumCols; j++) {
        stringBuilder.append(j != mNumCols - 1 ? "0 " : "0\n");
      }

      context.write(NullWritable.get(), new Text(stringBuilder.toString()));
    }

  }

}
