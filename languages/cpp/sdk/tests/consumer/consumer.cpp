// A consumer of the installed package: it includes the worker's, the host's and the client's
// headers, and uses a symbol of each library, so the link of each is checked. Exits 0 when every call
// answers as its header says.
#include <cstring>

#include "daedalus_client.hpp"
#include "daedalus_host.hpp"
#include "daedalus_worker.hpp"

int main() {
    dd::Types types;
    // The worker's loop is in the Rust library, so taking its address links that library: the
    // header's own type tables are inline and link nothing.
    int (*volatile serve)(const dd_node*, size_t, const char*) = &dd_serve;
    const bool worker = serve != nullptr && dd::scalar(DD_I64) != nullptr && types.scalar(DD_STRING) != nullptr;
    const bool host = std::strcmp(daedalus_status_name(DAEDALUS_OK), "DAEDALUS_OK") == 0;
    const bool client = std::strcmp(daedalus_client_status_name(DAEDALUS_CLIENT_OK), "DAEDALUS_CLIENT_OK") == 0;
    return worker && host && client ? 0 : 1;
}
