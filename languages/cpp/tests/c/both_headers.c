/* The host header and the worker header in one translation unit: each has its own guard, so both
 * declare their names and the program links. */
#include "daedalus.h"
#include "daedalus_worker.h"

int main(void) {
    return (DAEDALUS_OK == 0 && DD_K_SCALAR == 0) ? 0 : 1;
}
