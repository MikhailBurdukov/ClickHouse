#pragma once
#include <Core/Types.h>

namespace ytsaurus
{


struct QueryParameter
{
    String name;
    String value;
};
using QueryParameters = std::vector<QueryParameter>;


struct IYtsaurusQuery
{
    virtual String getQueryName() = 0;
    virtual QueryParameters getQueryParameters() = 0;
    virtual ~IYtsaurusQuery() = default;
};

struct YtsaurusReadTableQuery : public IYtsaurusQuery
{
    YtsaurusReadTableQuery(const String& path_) : path(path_) {}

    String getQueryName() override
    {
        return "read_table";
    }

    QueryParameters getQueryParameters() override
    {
        return {{.name="path",.value=path}};
    }
    String path;
};
using YtsaurusQueryPtr = std::shared_ptr<IYtsaurusQuery>;

}
