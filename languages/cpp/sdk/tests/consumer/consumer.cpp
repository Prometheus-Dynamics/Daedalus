// A consumer of the installed package: it includes the worker's, the host's and the client's
// headers, and calls into each library, so the link of each is checked. Exits 0 when every call
// answers as its header says.
#include <cstring>

#include "daedalus_client.hpp"
#include "daedalus_host.hpp"
#include "daedalus_worker.hpp"

int main() {
    dd::Types types;
    const bool worker = dd::scalar(DD_I64) != nullptr && types.scalar(DD_STRING) != nullptr;
    const bool host = std::strcmp(daedalus_status_name(DAEDALUS_OK), "DAEDALUS_OK") == 0;
    const bool client = std::strcmp(daedalus_client_status_name(DAEDALUS_CLIENT_OK), "DAEDALUS_CLIENT_OK") == 0;
    return worker && host && client ? 0 : 1;
}
