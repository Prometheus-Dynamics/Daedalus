import { Buffer } from "node:buffer";

// Runtime module for `opencv_demo`.
// Requires an OpenCV binding such as `opencv4nodejs` available at runtime.

// eslint-disable-next-line import/no-unresolved
const cv = require("opencv4nodejs");

export function blur(img) {
  const encoding = String(img.encoding ?? "raw");
  const bytes = Buffer.from(String(img.data_b64), "base64");

  // Prefer the fast path: raw pixels (HWC, u8). Keep PNG only as a fallback.
  let mat;
  if (encoding === "raw") {
    const width = Number(img.width);
    const height = Number(img.height);
    const channels = Number(img.channels ?? 4);
    const ty = channels === 1 ? cv.CV_8UC1 : channels === 3 ? cv.CV_8UC3 : cv.CV_8UC4;
    // opencv4nodejs supports constructing Mats from a raw buffer.
    mat = new cv.Mat(height, width, ty, bytes);
  } else {
    mat = cv.imdecode(bytes);
  }

  const out = mat.gaussianBlur(new cv.Size(7, 7), 0);

  // Mark the frame so multi-language pipelines can visually confirm each stage ran.
  const w = Number(img.width);
  const h = Number(img.height);
  try {
    cv.rectangle(out, new cv.Point2(5, 5), new cv.Point2(Math.max(6, w - 6), Math.max(6, h - 6)), new cv.Vec(255, 255, 255, 255), 2);
    cv.putText(out, "NODE", new cv.Point2(12, Math.max(24, Math.floor(h / 10))), cv.FONT_HERSHEY_SIMPLEX, 0.9, new cv.Vec(255, 255, 255, 255), 2);
  } catch {
    // Some bindings expose these as instance methods; keep the example resilient.
    if (typeof out.drawRectangle === "function") {
      out.drawRectangle(new cv.Point2(5, 5), new cv.Point2(Math.max(6, w - 6), Math.max(6, h - 6)), new cv.Vec(255, 255, 255), 2);
    }
    if (typeof out.putText === "function") {
      out.putText("NODE", new cv.Point2(12, Math.max(24, Math.floor(h / 10))), cv.FONT_HERSHEY_SIMPLEX, 0.9, new cv.Vec(255, 255, 255), 2);
    }
  }

  let outBytes;
  let outEncoding;
  if (encoding === "raw" && typeof out.getData === "function") {
    outBytes = out.getData();
    outEncoding = "raw";
  } else {
    outBytes = cv.imencode(".png", out);
    outEncoding = "png";
  }
  return {
    data_b64: Buffer.from(outBytes).toString("base64"),
    width: Number(img.width),
    height: Number(img.height),
    channels: Number(img.channels ?? 4),
    dtype: String(img.dtype ?? "u8"),
    layout: String(img.layout ?? "HWC"),
    encoding: outEncoding,
  };
}
