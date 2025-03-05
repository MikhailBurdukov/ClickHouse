#include "config.h"

#if USE_YTSAURUS
#include <memory>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/SortNode.h>
#include <Formats/BSONTypes.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/MongoDBSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/Ytsaurus/StorageYtsaurus.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/parseAddress.h>
#include <Common/ErrorCodes.h>
#include <Common/BSONCXXHelper.h>
#include <Core/Settings.h>
#include <Core/Joins.h>
#include <Processors/Sources/YtsaurusSource.h>
#include <Core/Ytsaurus/YtsaurusClient.h>
#include <Common/parseRemoteDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageYtsaurus::StorageYtsaurus(
    const StorageID & table_id_,
    YtsaurusStorageConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , configuration{std::move(configuration_)}
    , log(getLogger(" (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageYtsaurus::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    ytsaurus::YtsaurusClient::ConnectionInfo connection_info{.proxy = configuration.host, .proxy_port = configuration.port};
    ytsaurus::YtsaurusClientPtr client = std::make_unique<ytsaurus::YtsaurusClient>(connection_info);

    auto ptr = YtsaurusSourceFactory::createSource(std::move(client), configuration.path, sample_block, max_block_size);

    return Pipe(ptr);
}

YtsaurusStorageConfiguration StorageYtsaurus::getConfiguration(ASTs engine_args, ContextPtr context)
{
    YtsaurusStorageConfiguration configuration;
    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);
    if (engine_args.size() == 2)
    {
        auto addresses =  parseRemoteDescriptionForExternalDatabase(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 1, ytsaurus::DEFAULT_PROXY_PORT);
        configuration.host = addresses[0].first;
        configuration.port = addresses[0].second;
        configuration.path = checkAndGetLiteralArgument<String>(engine_args[1], "path");
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect Ytsarurus table schema");
    return configuration;
}

void registerStorageYtsaurus(StorageFactory & factory)
{
    factory.registerStorage("Ytsaurus", [](const StorageFactory::Arguments & args)
    {
        return std::make_shared<StorageYtsaurus>(
            args.table_id,
            StorageYtsaurus::getConfiguration(args.engine_args, args.getLocalContext()),
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::YTSAURUS,
    });
}

}
#endif
