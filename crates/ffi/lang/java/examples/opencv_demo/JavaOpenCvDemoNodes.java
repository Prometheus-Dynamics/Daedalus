package daedalus.examples;

import daedalus.annotations.In;
import daedalus.annotations.Node;
import daedalus.annotations.Out;

import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenCV image demo nodes.
 *
 * <p>This example requires OpenCV Java bindings (`org.opencv.*`) on the classpath and the native
 * OpenCV library available for `System.loadLibrary`.
 */
public final class JavaOpenCvDemoNodes {
  private JavaOpenCvDemoNodes() {}

  private static volatile boolean LOADED = false;

  private static void ensureLoaded() {
    if (LOADED) {
      return;
    }
    synchronized (JavaOpenCvDemoNodes.class) {
      if (!LOADED) {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        LOADED = true;
      }
    }
  }

  private static Map<String, Object> imageTy() {
    return daedalus.manifest.Types.structTy(
        List.of(
            new daedalus.manifest.Types.Field("data_b64", daedalus.manifest.Types.stringTy()),
            new daedalus.manifest.Types.Field("width", daedalus.manifest.Types.intTy()),
            new daedalus.manifest.Types.Field("height", daedalus.manifest.Types.intTy()),
            new daedalus.manifest.Types.Field("channels", daedalus.manifest.Types.intTy()),
            new daedalus.manifest.Types.Field("dtype", daedalus.manifest.Types.stringTy()),
            new daedalus.manifest.Types.Field("layout", daedalus.manifest.Types.stringTy()),
            new daedalus.manifest.Types.Field("encoding", daedalus.manifest.Types.stringTy())));
  }

  @Node(id = "demo_java_opencv:blur", label = "OpenCV Blur")
  @Out(index = 0, name = "out", tyRef = "daedalus.examples.JavaOpenCvDemoNodes#imageTy")
  public static Map<String, Object> blur(
      @In(name = "img", tyRef = "daedalus.examples.JavaOpenCvDemoNodes#imageTy") Map<String, Object> img)
      throws Exception {
    ensureLoaded();
    String encoding = String.valueOf(img.getOrDefault("encoding", "raw"));
    byte[] bytes = Base64.getDecoder().decode((String) img.get("data_b64"));

    Mat mat;
    if ("raw".equalsIgnoreCase(encoding)) {
      int width = ((Number) img.get("width")).intValue();
      int height = ((Number) img.get("height")).intValue();
      int channels = ((Number) img.getOrDefault("channels", 4)).intValue();

      int ty =
          channels == 1
              ? CvType.CV_8UC1
              : channels == 3 ? CvType.CV_8UC3 : CvType.CV_8UC4;

      mat = new Mat(height, width, ty);
      mat.put(0, 0, bytes);
    } else {
      MatOfByte buf = new MatOfByte(bytes);
      mat = Imgcodecs.imdecode(buf, Imgcodecs.IMREAD_UNCHANGED);
    }

    Mat dst = new Mat();
    Imgproc.GaussianBlur(mat, dst, new Size(7, 7), 0);

    Imgproc.rectangle(
        dst,
        new org.opencv.core.Point(5, 5),
        new org.opencv.core.Point(Math.max(6, dst.cols() - 6), Math.max(6, dst.rows() - 6)),
        new org.opencv.core.Scalar(255, 255, 255, 255),
        2);
    Imgproc.putText(
        dst,
        "JAVA",
        new org.opencv.core.Point(12, Math.max(24, dst.rows() / 10)),
        Imgproc.FONT_HERSHEY_SIMPLEX,
        0.9,
        new org.opencv.core.Scalar(255, 255, 255, 255),
        2);

    byte[] outBytes;
    String outEncoding;
    if ("raw".equalsIgnoreCase(encoding)) {
      int rows = dst.rows();
      int cols = dst.cols();
      int channels = dst.channels();
      int len = rows * cols * channels; // CV_8U-only in this demo
      outBytes = new byte[len];
      dst.get(0, 0, outBytes);
      outEncoding = "raw";
    } else {
      MatOfByte outBuf = new MatOfByte();
      Imgcodecs.imencode(".png", dst, outBuf);
      outBytes = outBuf.toArray();
      outEncoding = "png";
    }

    Map<String, Object> out = new LinkedHashMap<>();
    out.put("data_b64", Base64.getEncoder().encodeToString(outBytes));
    out.put("width", ((Number) img.get("width")).intValue());
    out.put("height", ((Number) img.get("height")).intValue());
    out.put("channels", ((Number) img.getOrDefault("channels", 4)).intValue());
    out.put("dtype", String.valueOf(img.getOrDefault("dtype", "u8")));
    out.put("layout", String.valueOf(img.getOrDefault("layout", "HWC")));
    out.put("encoding", outEncoding);
    return out;
  }
}
