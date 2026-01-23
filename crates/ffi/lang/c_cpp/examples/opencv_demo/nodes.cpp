// Daedalus C++ OpenCV demo node.
//
// This example requires OpenCV at build time and runtime.
// It demonstrates using `cv::Mat` + `cv::GaussianBlur` inside a C++ plugin node.

#include "daedalus.hpp"

#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

#include <cstring>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {

static std::vector<uint8_t> b64_decode(const std::string& in) {
  static const int8_t kDec[256] = {
      -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
      -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,52,53,54,55,56,57,58,59,60,61,-1,-1,-1,64,-1,-1,
      -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
      -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
      -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
      -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
      -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
      -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
  };

  std::vector<uint8_t> out;
  out.reserve((in.size() * 3) / 4);
  uint32_t val = 0;
  int valb = -8;
  for (unsigned char c : in) {
    int8_t d = kDec[c];
    if (d == -1) continue;
    if (d == 64) break;
    val = (val << 6) | (uint32_t)d;
    valb += 6;
    if (valb >= 0) {
      out.push_back((uint8_t)((val >> valb) & 0xFF));
      valb -= 8;
    }
  }
  return out;
}

static std::string b64_encode(const uint8_t* data, size_t len) {
  static const char* kEnc =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((len + 2) / 3) * 4);
  size_t i = 0;
  while (i + 3 <= len) {
    uint32_t v = ((uint32_t)data[i] << 16) | ((uint32_t)data[i + 1] << 8) | (uint32_t)data[i + 2];
    out.push_back(kEnc[(v >> 18) & 63]);
    out.push_back(kEnc[(v >> 12) & 63]);
    out.push_back(kEnc[(v >> 6) & 63]);
    out.push_back(kEnc[v & 63]);
    i += 3;
  }
  const size_t rem = len - i;
  if (rem == 1) {
    uint32_t v = ((uint32_t)data[i] << 16);
    out.push_back(kEnc[(v >> 18) & 63]);
    out.push_back(kEnc[(v >> 12) & 63]);
    out.push_back('=');
    out.push_back('=');
  } else if (rem == 2) {
    uint32_t v = ((uint32_t)data[i] << 16) | ((uint32_t)data[i + 1] << 8);
    out.push_back(kEnc[(v >> 18) & 63]);
    out.push_back(kEnc[(v >> 12) & 63]);
    out.push_back(kEnc[(v >> 6) & 63]);
    out.push_back('=');
  }
  return out;
}

static std::optional<std::string> json_get_string(const std::string& json, const char* key) {
  size_t pos = 0;
  if (!daedalus::find_key(json.c_str(), key, pos)) return std::nullopt;
  size_t i = pos;
  while (json[i] && json[i] != ':') i++;
  if (!json[i]) return std::nullopt;
  i++;
  std::string out;
  if (!daedalus::parse_string(json.c_str(), i, out)) return std::nullopt;
  return out;
}

static std::optional<int64_t> json_get_i64(const std::string& json, const char* key) {
  size_t pos = 0;
  if (!daedalus::find_key(json.c_str(), key, pos)) return std::nullopt;
  size_t i = pos;
  while (json[i] && json[i] != ':') i++;
  if (!json[i]) return std::nullopt;
  i++;
  double out = 0.0;
  if (!daedalus::parse_number(json.c_str(), i, out)) return std::nullopt;
  return (int64_t)out;
}

static std::string opencv_blur_image_json(const std::string& img_json) {
  const std::string data_b64 = json_get_string(img_json, "data_b64").value_or("");
  const std::string encoding = json_get_string(img_json, "encoding").value_or("raw");
  const int64_t width = json_get_i64(img_json, "width").value_or(0);
  const int64_t height = json_get_i64(img_json, "height").value_or(0);
  const int64_t channels = json_get_i64(img_json, "channels").value_or(4);

  if (width <= 0 || height <= 0) {
    return img_json;
  }

  const std::vector<uint8_t> bytes = b64_decode(data_b64);

  cv::Mat mat;
  if (encoding == "png") {
    mat = cv::imdecode(bytes, cv::IMREAD_UNCHANGED);
  } else {
    const int ty = (channels == 1) ? CV_8UC1 : (channels == 3) ? CV_8UC3 : CV_8UC4;
    mat = cv::Mat((int)height, (int)width, ty);
    const size_t need = (size_t)height * (size_t)width * (size_t)channels;
    if (bytes.size() >= need) {
      std::memcpy(mat.data, bytes.data(), need);
    }
  }

  cv::Mat out;
  cv::GaussianBlur(mat, out, cv::Size(7, 7), 0);
  cv::rectangle(out,
                cv::Point(5, 5),
                cv::Point(std::max(6, out.cols - 6), std::max(6, out.rows - 6)),
                cv::Scalar(255, 255, 255, 255),
                2);
  cv::putText(out,
              "C++",
              cv::Point(12, std::max(24, out.rows / 10)),
              cv::FONT_HERSHEY_SIMPLEX,
              0.9,
              cv::Scalar(255, 255, 255, 255),
              2,
              cv::LINE_AA);

  if (!out.isContinuous()) out = out.clone();
  const size_t out_len = (size_t)out.total() * (size_t)out.elemSize();
  const std::string out_b64 = b64_encode(out.data, out_len);

  std::ostringstream ss;
  ss << "{";
  ss << "\"data_b64\":\"" << out_b64 << "\",";
  ss << "\"width\":" << out.cols << ",";
  ss << "\"height\":" << out.rows << ",";
  ss << "\"channels\":" << out.channels() << ",";
  ss << "\"dtype\":\"u8\",";
  ss << "\"layout\":\"HWC\",";
  ss << "\"encoding\":\"raw\"";
  ss << "}";
  return ss.str();
}

}  // namespace

DAEDALUS_NODE_WITH(
    "demo_cpp_opencv:blur",
    opencv_blur_image_json,
    DAEDALUS_PORTS(img),
    DAEDALUS_PORTS(out),
    {
      def.set_label("OpenCV Blur");
      def.set_metadata_json("{\"requires\":\"opencv\"}");
    })

DAEDALUS_PLUGIN("demo_cpp_opencv", "0.1.1", "OpenCV image demo")
