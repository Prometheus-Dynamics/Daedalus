#include <daedalus.hpp>

#include "src/showcase.cpp"

int main() {
  daedalus::PackageBuilder::from_plugin("ffi_showcase")
      .shared_library("build/libffi_showcase.so")
      .source_file("src/showcase.cpp")
      .write("plugin.json");
}
