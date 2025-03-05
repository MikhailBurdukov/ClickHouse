#include "config.h"

#if USE_YTSAURUS
#    include <memory>
#    include <IO/HTTPHeaderEntries.h>
#    include <IO/ReadHelpers.h>
#    include <IO/ReadWriteBufferFromHTTP.h>
#    include "YtsaurusClient.h"
//#include <Interpreters/Context.h>
#    include <Poco/Net/HTTPRequest.h>


namespace ytsaurus
{

YtsaurusClient::YtsaurusClient(const ConnectionInfo & connection_info_, size_t num_tries_)
    : connection_info(connection_info_), num_tries(num_tries_), log(getLogger("YtsaurusClient"))
{
    base_uri.setHost(connection_info.proxy);
    base_uri.setPort(static_cast<int>(connection_info.proxy_port));
    base_uri.setScheme("http");
}


DB::ReadBufferPtr YtsaurusClient::readTable(const String & path)
{
    YtsaurusQueryPtr read_table_query(new YtsaurusReadTableQuery(path));
    return execWithRetry(read_table_query);
}

DB::ReadBufferPtr YtsaurusClient::execWithRetry(const YtsaurusQueryPtr query)
{
    Poco::URI uri = base_uri;
    uri.setPath(fmt::format("/api/{}/{}", connection_info.api_version, query->getQueryName()));

    for (const auto & query_param : query->getQueryParameters())
    {
        uri.addQueryParameter(query_param.name, query_param.value);
    }

    DB::HTTPHeaderEntries http_headers{
        {"Accept", "application/json"},
    };

    /// RN i don't know how to not keep whole table in memory in general case.
    /// For yt dynamic tables we can make smth like paginator, but what about static tables?
    /// Does any one using big static tables?
    ///
    LOG_TRACE(log, "URI {} , query type {}", uri.toString(), query->getQueryName());
    Poco::Net::HTTPBasicCredentials creds;
    auto buf = DB::BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(DB::HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_GET)
                   .withHeaders(http_headers)
                   .create(creds);

    return DB::ReadBufferPtr(std::move(buf));
}

}
#endif
