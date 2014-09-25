import java.io.*;
import java.util.Scanner;

public class ImageFilter {

  public static void main(String[] args) throws Exception {
    int[][] image = readImage("Fox_g.txt");
    int[][] filteredImage = filterImage(image);
    saveImage(filteredImage, "Fox_filtered.txt");
  }

  /**
   * Applies the filter to the given image, returning a new (filtered) image.
   */
  private static int[][] filterImage(int[][] image) {
    int nRows = image.length;
    int nCols = image[0].length;

    // Initialize borders
    int[][] filteredImage = new int[nRows][nCols];
    for (int j = 0; j < nCols; j++) {
      filteredImage[0][j] = 0;
      filteredImage[nRows - 1][j] = 0;
    }
    for (int i = 0; i < nRows; i++) {
      filteredImage[i][0] = 0;
      filteredImage[i][nCols - 1] = 0;
    }

    for (int i = 1; i < nRows - 1; i++) {
      for (int j = 1; j < nCols - 1; j++) {
        filteredImage[i][j] = filterWindow(
          image[i - 1][j - 1], image[i - 1][j], image[i - 1][j + 1],
          image[i    ][j - 1], image[i    ][j], image[i    ][j + 1],
          image[i + 1][j - 1], image[i + 1][j], image[i + 1][j + 1]
        );
      }
    }

    return filteredImage;
  }

  /**
   * Applies the Sobel filter to a 3x3 window of the image. Parameters:
   * t_ top pixel
   * m_ middle pixel
   * b_ bottom pixel
   * _l left pixel
   * _c center pixel
   * _r right pixel
   */
  private static int filterWindow(
      int tl, int tc, int tr,
      int ml, int mc, int mr,
      int bl, int bc, int br) {
    double sum, sum_x = 0.0, sum_y = 0.0;

    // Horizontal component
    sum_x += 1.0 * tl + 0.0 * tc - 1.0 * tr;
    sum_x += 2.0 * ml + 0.0 * mc - 2.0 * mr;
    sum_x += 1.0 * bl + 0.0 * bc - 1.0 * br;

    // Vertical component
    sum_y +=  1.0 * tl + 2.0 * tc + 1.0 * tr;
    sum_y +=  0.0 * ml + 0.0 * mc + 0.0 * mr;
    sum_y += -1.0 * bl - 2.0 * bc - 1.0 * br;

    // Combine both
    sum = Math.sqrt(sum_x * sum_x + sum_y * sum_y);

    if (sum < 0.0) return 0;
    if (sum > 255.0) return 255;
    return (int)sum;
  }

  /**
   * Reads a gray-scale image matrix from file into memory.
   */
  private static int[][] readImage(String filePath) throws FileNotFoundException {
    Scanner scanner = new Scanner(new File(filePath));

    int nRows, nCols;
    nRows = scanner.nextInt();
    nCols = scanner.nextInt();

    int[][] image = new int[nRows][nCols];
    for (int i = 0; i < nRows; i++) {
      for (int j = 0; j < nCols; j++) {
        image[i][j] = scanner.nextInt();
      }
    }

    scanner.close();
    return image;
  }

  /**
   * Saves a gray-scale image matrix from memory to disk.
   */
  private static void saveImage(int[][] image, String filePath) throws IOException {
    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));

    bufferedWriter.write(image.length + " " + image[0].length);
    bufferedWriter.newLine();

    for (int i = 0; i < image.length; i++) {
      for (int j = 0; j < image[0].length; j++) {
        bufferedWriter.write((j == 0 ? "" : " ") + Integer.toString(image[i][j]));
      }
      bufferedWriter.newLine();
    }

    bufferedWriter.flush();
    bufferedWriter.close();
  }

}