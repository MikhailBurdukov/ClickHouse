#include <Interpreters/SessionLog.h>

#include <base/getFQDNOrHostName.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Common/DateLUTImpl.h>
#include <Core/Settings.h>
#include <Core/Protocol.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Common/IPv6ToBinary.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Access/SettingsProfilesInfo.h>
#include <Interpreters/Context.h>

#include <Poco/Net/SocketAddress.h>

#include <cassert>


namespace
{
using namespace DB;

auto eventTime()
{
    const auto finish_time = std::chrono::system_clock::now();

    return std::make_pair(timeInSeconds(finish_time), timeInMicroseconds(finish_time));
}

using AuthType = AuthenticationType;
using Interface = ClientInfo::Interface;

void fillColumnArray(const Strings & data, IColumn & column)
{
    auto & array = typeid_cast<ColumnArray &>(column);
    size_t size = 0;
    auto & data_col = array.getData();
    for (const auto & name : data)
    {
        data_col.insertData(name.data(), name.size());
        ++size;
    }
    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + size);
}

}

namespace DB
{

SessionLogElement::SessionLogElement(const UUID & auth_id_, Type type_)
    : auth_id(auth_id_),
      type(type_)
{
    std::tie(event_time, event_time_microseconds) = eventTime();
}

ColumnsDescription SessionLogElement::getColumnsDescription()
{
    auto event_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"LoginFailure",           static_cast<Int8>(SESSION_LOGIN_FAILURE)},
            {"LoginSuccess",           static_cast<Int8>(SESSION_LOGIN_SUCCESS)},
            {"Logout",                 static_cast<Int8>(SESSION_LOGOUT)}
        });

#define AUTH_TYPE_NAME_AND_VALUE(v) std::make_pair(toString(v), static_cast<Int8>(v))
    auto identified_with_column = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            AUTH_TYPE_NAME_AND_VALUE(AuthType::NO_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::PLAINTEXT_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::SHA256_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::DOUBLE_SHA1_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::LDAP),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::JWT),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::KERBEROS),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::SSH_KEY),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::SSL_CERTIFICATE),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::BCRYPT_PASSWORD),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::HTTP),
            AUTH_TYPE_NAME_AND_VALUE(AuthType::SCRAM_SHA256_PASSWORD),
        });
#undef AUTH_TYPE_NAME_AND_VALUE
    static_assert(static_cast<int>(AuthenticationType::MAX) == 12);

    auto interface_type_column = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"TCP",                    static_cast<Int8>(Interface::TCP)},
            {"HTTP",                   static_cast<Int8>(Interface::HTTP)},
            {"gRPC",                   static_cast<Int8>(Interface::GRPC)},
            {"MySQL",                  static_cast<Int8>(Interface::MYSQL)},
            {"PostgreSQL",             static_cast<Int8>(Interface::POSTGRESQL)},
            {"Local",                  static_cast<Int8>(Interface::LOCAL)},
            {"TCP_Interserver",        static_cast<Int8>(Interface::TCP_INTERSERVER)},
            {"Prometheus",             static_cast<Int8>(Interface::PROMETHEUS)},
            {"Background",             static_cast<Int8>(Interface::BACKGROUND)},
        });
    static_assert(magic_enum::enum_count<Interface>() == 9, "Please update the array above to match the enum.");

    auto lc_string_datatype = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto settings_type_column = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes({
                // setting name
                lc_string_datatype,
                // value
                std::make_shared<DataTypeString>()
            })));

    return ColumnsDescription
    {
        {"hostname", lc_string_datatype, "Hostname of the server executing the query."},
        {"type", std::move(event_type), "Login/logout result. Possible values: "
            "LoginFailure — Login error. "
            "LoginSuccess — Successful login. "
            "Logout — Logout from the system."},
        {"auth_id", std::make_shared<DataTypeUUID>(), "Authentication ID, which is a UUID that is automatically generated each time user logins."},
        {"session_id", std::make_shared<DataTypeString>(), "Session ID that is passed by client via HTTP interface."},
        {"event_date", std::make_shared<DataTypeDate>(), "Login/logout date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Login/logout time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Login/logout starting time with microseconds precision."},

        {"user", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "User name."},
        {"auth_type", std::make_shared<DataTypeNullable>(std::move(identified_with_column)), "The authentication type."},

        {"profiles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of profiles set for all roles and/or users."},
        {"roles", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of roles to which the profile is applied."},
        {"settings", std::move(settings_type_column), "Settings that were changed when the client logged in/out."},

        {"client_address", DataTypeFactory::instance().get("IPv6"), "The IP address that was used to log in/out."},
        {"client_port", std::make_shared<DataTypeUInt16>(), "The client port that was used to log in/out."},
        {"interface", std::move(interface_type_column), "The interface from which the login was initiated."},

        {"client_hostname", std::make_shared<DataTypeString>(), "The hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", std::make_shared<DataTypeString>(), "The clickhouse-client or another TCP client name."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "The major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "The minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},

        {"failure_reason", std::make_shared<DataTypeString>(), "The exception message containing the reason for the login/logout failure."},
    };
}

void SessionLogElement::appendToBlock(MutableColumns & columns) const
{
    assert(type >= SESSION_LOGIN_FAILURE && type <= SESSION_LOGOUT);
    assert(user_identified_with >= AuthenticationType::NO_PASSWORD && user_identified_with <= AuthenticationType::MAX);

    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(type);
    columns[i++]->insert(auth_id);
    columns[i++]->insert(session_id);
    columns[i++]->insert(static_cast<DayNum>(DateLUT::instance().toDayNum(event_time).toUnderType()));
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    assert((user && user_identified_with) || client_info.interface == ClientInfo::Interface::TCP_INTERSERVER);
    columns[i++]->insert(user ? Field(*user) : Field());
    columns[i++]->insert(user_identified_with ? Field(*user_identified_with) : Field());

    fillColumnArray(profiles, *columns[i++]);
    fillColumnArray(roles, *columns[i++]);

    {
        auto & settings_array_col = assert_cast<ColumnArray &>(*columns[i++]);
        auto & settings_tuple_col = assert_cast<ColumnTuple &>(settings_array_col.getData());
        auto & names_col = *settings_tuple_col.getColumnPtr(0)->assumeMutable();
        auto & values_col = assert_cast<ColumnString &>(*settings_tuple_col.getColumnPtr(1)->assumeMutable());

        for (const auto & kv : settings)
        {
            names_col.insert(kv.first);
            values_col.insert(kv.second);
        }

        auto & offsets = settings_array_col.getOffsets();
        offsets.push_back(settings_tuple_col.size());
    }

    columns[i++]->insertData(IPv6ToBinary(client_info.current_address->host()).data(), 16);
    columns[i++]->insert(client_info.current_address->port());

    columns[i++]->insert(client_info.interface);

    columns[i++]->insertData(client_info.client_hostname.data(), client_info.client_hostname.length());
    columns[i++]->insertData(client_info.client_name.data(), client_info.client_name.length());
    columns[i++]->insert(client_info.client_tcp_protocol_version);
    columns[i++]->insert(client_info.client_version_major);
    columns[i++]->insert(client_info.client_version_minor);
    columns[i++]->insert(client_info.client_version_patch);

    columns[i++]->insertData(auth_failure_reason.data(), auth_failure_reason.length());
}

void SessionLog::addLoginSuccess(const UUID & auth_id,
                                 const String & session_id,
                                 const Settings & settings,
                                 const ContextAccessPtr & access,
                                 const ClientInfo & client_info,
                                 const UserPtr & login_user,
                                 const AuthenticationData & user_authenticated_with)
{
    SessionLogElement log_entry(auth_id, SESSION_LOGIN_SUCCESS);
    log_entry.client_info = client_info;

    if (login_user)
    {
        log_entry.user = login_user->getName();
        log_entry.user_identified_with = user_authenticated_with.getType();
    }

    log_entry.external_auth_server = user_authenticated_with.getLDAPServerName();


    log_entry.session_id = session_id;

    if (const auto roles_info = access->getRolesInfo())
        log_entry.roles = roles_info->getCurrentRolesNames();

    if (const auto profile_info = access->getDefaultProfileInfo())
        log_entry.profiles = profile_info->getProfileNames();

    SettingsChanges changes = settings.changes();
    for (const auto & change : changes)
        log_entry.settings.emplace_back(change.name, Settings::valueToStringUtil(change.name, change.value));

    add(std::move(log_entry));
}

void SessionLog::addLoginFailure(
        const UUID & auth_id,
        const ClientInfo & info,
        const std::optional<String> & user,
        const Exception & reason)
{
    SessionLogElement log_entry(auth_id, SESSION_LOGIN_FAILURE);

    log_entry.user = user;
    log_entry.auth_failure_reason = reason.message();
    log_entry.client_info = info;
    log_entry.user_identified_with = AuthenticationType::NO_PASSWORD;

    add(std::move(log_entry));
}

void SessionLog::addLogOut(
    const UUID & auth_id,
    const UserPtr & login_user,
    const AuthenticationData & user_authenticated_with,
    const ClientInfo & client_info)
{
    auto log_entry = SessionLogElement(auth_id, SESSION_LOGOUT);
    if (login_user)
    {
        log_entry.user = login_user->getName();
        log_entry.user_identified_with = user_authenticated_with.getType();
    }
    log_entry.external_auth_server = user_authenticated_with.getLDAPServerName();
    log_entry.client_info = client_info;

    add(std::move(log_entry));
}

}
