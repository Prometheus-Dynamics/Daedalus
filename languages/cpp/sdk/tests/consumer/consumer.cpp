#include "daedalus_worker.hpp"
int main(int argc, char**) { return argc > 99 ? dd_serve(nullptr, 0, "none") : (dd::scalar(DD_I64) != nullptr ? 0 : 1); }
