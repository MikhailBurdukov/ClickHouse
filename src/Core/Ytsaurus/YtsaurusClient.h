#pragma once

#include "config.h"

#if USE_YTSAURUS

#    include <Core/Types.h>
#    include <IO/ReadBuffer.h>
#    include <boost/noncopyable.hpp>
#    include <Poco/URI.h>
#    include <Common/Logger.h>
#    include <Common/logger_useful.h>
#    include "YtsaurusQueries.h"


namespace Poco
{
class Logger;
}

namespace ytsaurus
{

const uint16_t DEFAULT_PROXY_PORT = 80;

class YtsaurusClient : private boost::noncopyable
{
public:

    struct ConnectionInfo
    {
        String proxy;
        uint16_t proxy_port = DEFAULT_PROXY_PORT;
        String auth_token;
        String api_version = "v3";

        Poco::URI getBaseUri();
    };

    explicit YtsaurusClient(const ConnectionInfo & connection_info_, size_t num_tries = 3);

    const ConnectionInfo & getConnectionInfo() { return connection_info; }
    DB::ReadBufferPtr readTable(const String & path);

private:
    DB::ReadBufferPtr execWithRetry(const YtsaurusQueryPtr query);

    ConnectionInfo connection_info;
    size_t num_tries;
    Poco::URI base_uri;

    LoggerPtr log;
};

using YtsaurusClientPtr = std::unique_ptr<YtsaurusClient>;

}

#endif
