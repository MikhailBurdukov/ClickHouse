#include <iostream>
#include <filesystem>
#include <boost/program_options.hpp>
#include <Common/filesystemHelpers.h>

#include <sys/stat.h>
#include <pwd.h>

#if defined(OS_LINUX)
    #include <syscall.h>
    #include <linux/capability.h>
#endif

#if defined(OS_DARWIN)
    #include <mach-o/dyld.h>
#endif

#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/formatReadable.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/OpenSSLHelpers.h>
#include <base/sleep.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/MMapReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include <IO/Operators.h>
#include <IO/Ask.h>
#include <readpassphrase.h>

#include <Poco/Util/XMLConfiguration.h>

#include <incbin.h>

#include "config.h"

/// Embedded configuration files used inside the install program
INCBIN(resource_config_xml, SOURCE_DIR "/programs/server/config.xml");
INCBIN(resource_users_xml, SOURCE_DIR "/programs/server/users.xml");


/** This tool can be used to install ClickHouse without a deb/rpm/tgz package, having only "clickhouse" binary.
  * It also allows to avoid dependency on systemd, upstart, SysV init.
  *
  * The following steps are performed:
  *
  * - copying the binary to binary directory (/usr/bin/)
  * - creation of symlinks for tools.
  * - creation of clickhouse user and group.
  * - creation of config directory (/etc/clickhouse-server/).
  * - creation of default configuration files.
  * - creation of a directory for logs (/var/log/clickhouse-server/).
  * - creation of a data directory if not exists.
  * - setting a password for default user.
  * - choose an option to listen connections.
  * - changing the ownership and mode of the directories.
  * - setting capabilities for binary.
  * - setting ulimits for the user.
  * - (todo) put service in cron.
  *
  * It does not install clickhouse-odbc-bridge.
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int SYSTEM_ERROR;
    extern const int NOT_ENOUGH_SPACE;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_KILL;
    extern const int BAD_ARGUMENTS;
}

}

#if defined(OS_DARWIN)
/// Until createUser() and createGroup() are implemented, only sudo-less installations are supported/default for macOS.
static constexpr auto DEFAULT_CLICKHOUSE_SERVER_USER = "";
static constexpr auto DEFAULT_CLICKHOUSE_SERVER_GROUP = "";
static constexpr auto DEFAULT_CLICKHOUSE_BRIDGE_USER = "";
static constexpr auto DEFAULT_CLICKHOUSE_BRIDGE_GROUP = "";
#else
static constexpr auto DEFAULT_CLICKHOUSE_SERVER_USER = "clickhouse";
static constexpr auto DEFAULT_CLICKHOUSE_SERVER_GROUP = "clickhouse";
static constexpr auto DEFAULT_CLICKHOUSE_BRIDGE_USER = "clickhouse-bridge";
static constexpr auto DEFAULT_CLICKHOUSE_BRIDGE_GROUP = "clickhouse-bridge";
#endif

using namespace DB;
namespace po = boost::program_options;
namespace fs = std::filesystem;


static auto executeScript(const std::string & command, bool throw_on_error = false)
{
    auto sh = ShellCommand::execute(command);

    WriteBufferFromFileDescriptor wb_stdout(STDOUT_FILENO);
    copyData(sh->out, wb_stdout);
    wb_stdout.finalize();

    WriteBufferFromFileDescriptor wb_stderr(STDERR_FILENO);
    copyData(sh->err, wb_stderr);
    wb_stderr.finalize();

    if (throw_on_error)
    {
        sh->wait();
        return 0;
    }

    return sh->tryWait();
}

static bool filesEqual(std::string path1, std::string path2)
{
    MMapReadBufferFromFile in1(path1, 0);
    MMapReadBufferFromFile in2(path2, 0);

    /// memcmp is faster than hashing and comparing hashes
    return in1.buffer().size() == in2.buffer().size()
        && 0 == memcmp(in1.buffer().begin(), in2.buffer().begin(), in1.buffer().size());
}

static void changeOwnership(const String & file_name, const String & user_name, const String & group_name = {}, bool recursive = true)
{
    if (!user_name.empty() || !group_name.empty())
    {
        std::string command = fmt::format("chown {} {}:{} '{}'", (recursive ? "-R" : ""), user_name, group_name, file_name);
        fmt::print(" {}\n", command);
        executeScript(command);
    }
}

static void createGroup(const String & group_name)
{
    if (!group_name.empty())
    {
#if defined(OS_DARWIN)
        // TODO: implement.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unable to create a group in macOS");
#elif defined(OS_FREEBSD)
        std::string command = fmt::format("pw groupadd {}", group_name);
        fmt::print(" {}\n", command);
        executeScript(command);
#else
        std::string command = fmt::format("groupadd -r {}", group_name);
        fmt::print(" {}\n", command);
        executeScript(command);
#endif
    }
}

static void createUser(const String & user_name, [[maybe_unused]] const String & group_name)
{
    if (!user_name.empty())
    {
#if defined(OS_DARWIN)
        // TODO: implement.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unable to create a user in macOS");
#elif defined(OS_FREEBSD)
        std::string command = group_name.empty()
            ? fmt::format("pw useradd -s /bin/false -d /nonexistent -n {}", user_name)
            : fmt::format("pw useradd -s /bin/false -d /nonexistent -g {} -n {}", group_name, user_name);
        fmt::print(" {}\n", command);
        executeScript(command);
#else
        std::string command = group_name.empty()
            ? fmt::format("useradd -r --shell /bin/false --home-dir /nonexistent --user-group {}", user_name)
            : fmt::format("useradd -r --shell /bin/false --home-dir /nonexistent -g {} {}", group_name, user_name);
        fmt::print(" {}\n", command);
        executeScript(command);
#endif
    }
}


static std::string formatWithSudo(std::string command, bool needed = true)
{
    if (!needed)
        return command;

#if defined(OS_FREEBSD)
    /// FreeBSD does not have 'sudo' installed.
    return fmt::format("su -m root -c '{}'", command);
#else
    return fmt::format("sudo {}", command);
#endif
}


int mainEntryClickHouseInstall(int argc, char ** argv)
{
    try
    {
        const char * start_hilite = "";
        const char * end_hilite = "";

        if (isatty(STDOUT_FILENO))
        {
            /// ANSI escape sequence for intense color in terminal.
            start_hilite = "\033[1m";
            end_hilite = "\033[0m";
        }

        po::options_description desc;
        desc.add_options()
            ("help,h", "produce help message")
            ("prefix", po::value<std::string>()->default_value("/"), "prefix for all paths")
#if defined (OS_DARWIN)
            /// https://stackoverflow.com/a/36734569/22422288
            ("binary-path", po::value<std::string>()->default_value("usr/local/bin"), "where to install binaries")
#else
            ("binary-path", po::value<std::string>()->default_value("usr/bin"), "where to install binaries")
#endif
            ("config-path", po::value<std::string>()->default_value("etc/clickhouse-server"), "where to install configs")
            ("log-path", po::value<std::string>()->default_value("var/log/clickhouse-server"), "where to create log directory")
            ("data-path", po::value<std::string>()->default_value("var/lib/clickhouse"), "directory for data")
            ("pid-path", po::value<std::string>()->default_value("var/run/clickhouse-server"), "directory for pid file")
            ("user", po::value<std::string>()->default_value(DEFAULT_CLICKHOUSE_SERVER_USER), "clickhouse user to create")
            ("group", po::value<std::string>()->default_value(DEFAULT_CLICKHOUSE_SERVER_GROUP), "clickhouse group to create")
            ("noninteractive,y", "run non-interactively")
            ("link", "create symlink to the binary instead of copying to binary-path")
        ;

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Install ClickHouse without .deb/.rpm/.tgz packages (having the binary only)\n\n";
            std::cout << "Usage: " << formatWithSudo(std::string(argv[0]) + " install [options]", getuid() != 0) << '\n';
            std::cout << desc << '\n';
            return 0;
        }

        /// We need to copy binary to the binary directory.
        /// The binary is currently run. We need to obtain its path from procfs (on Linux).

#if defined(OS_DARWIN)
        uint32_t path_length = 0;
        _NSGetExecutablePath(nullptr, &path_length);
        if (path_length <= 1)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot obtain path to the binary");

        std::string path(path_length, std::string::value_type());
        auto res = _NSGetExecutablePath(&path[0], &path_length);
        if (res != 0)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot obtain path to the binary");

        if (path.back() == '\0')
            path.pop_back();

        fs::path binary_self_path(path);
#elif defined(OS_FREEBSD)
        /// https://stackoverflow.com/questions/1023306/finding-current-executables-path-without-proc-self-exe
        fs::path binary_self_path = argc >= 1 ? argv[0] : "/proc/curproc/file";
#else
        fs::path binary_self_path = "/proc/self/exe";
#endif

        if (!fs::exists(binary_self_path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot obtain path to the binary from {}, file doesn't exist",
                            binary_self_path.string());

        fs::path binary_self_canonical_path = fs::canonical(binary_self_path);

        /// Copy binary to the destination directory.

        fs::path prefix = options["prefix"].as<std::string>();
        fs::path bin_dir = prefix / options["binary-path"].as<std::string>();

        fs::path main_bin_path = bin_dir / "clickhouse";
        fs::path main_bin_tmp_path = bin_dir / "clickhouse.new";
        fs::path main_bin_old_path = bin_dir / "clickhouse.old";

        size_t binary_size = fs::file_size(binary_self_path);

        bool old_binary_exists = fs::exists(main_bin_path);
        bool already_installed = false;

        if (options.count("link"))
        {
            if (old_binary_exists)
            {
                bool is_symlink = FS::isSymlink(main_bin_path);
                fs::path points_to;
                if (is_symlink)
                    points_to = fs::weakly_canonical(FS::readSymlink(main_bin_path));

                if (is_symlink && points_to == binary_self_canonical_path)
                {
                    already_installed = true;
                }
                else
                {
                    if (!is_symlink)
                    {
                        fmt::print("File {} already exists but it's not a symlink. Will rename to {}.\n",
                                   main_bin_path.string(), main_bin_old_path.string());
                        fs::rename(main_bin_path, main_bin_old_path);
                    }
                    else if (points_to != main_bin_path)
                    {
                        fmt::print("Symlink {} already exists but it points to {}. Will replace the old symlink to {}.\n",
                                   main_bin_path.string(), points_to.string(), binary_self_canonical_path.string());
                        (void)fs::remove(main_bin_path);
                    }
                }
            }

            if (!already_installed)
            {
                if (!fs::exists(bin_dir))
                {
                    fmt::print("Creating binary directory {}.\n", bin_dir.string());
                    fs::create_directories(bin_dir);
                }

                fmt::print("Creating symlink {} to {}.\n", main_bin_path.string(), binary_self_canonical_path.string());
                fs::create_symlink(binary_self_canonical_path, main_bin_path);

                if (0 != chmod(binary_self_canonical_path.string().c_str(), S_IRUSR | S_IRGRP | S_IROTH | S_IXUSR | S_IXGRP | S_IXOTH))
                    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot chmod {}", binary_self_canonical_path.string());
            }
        }
        else
        {
            bool is_symlink = FS::isSymlink(main_bin_path);

            if (!is_symlink)
            {
                /// Check if the binary is the same file (already installed).
                if (old_binary_exists && binary_self_canonical_path == fs::canonical(main_bin_path))
                {
                    already_installed = true;
                    fmt::print("ClickHouse binary is already located at {}\n", main_bin_path.string());
                }
                /// Check if binary has the same content.
                else if (old_binary_exists && binary_size == fs::file_size(main_bin_path))
                {
                    fmt::print("Found already existing ClickHouse binary at {} having the same size. Will check its contents.\n",
                        main_bin_path.string());

                    if (filesEqual(binary_self_path.string(), main_bin_path.string()))
                    {
                        already_installed = true;
                        fmt::print("ClickHouse binary is already located at {} and it has the same content as {}\n",
                            main_bin_path.string(), binary_self_canonical_path.string());
                    }
                }
            }

            if (already_installed)
            {
                if (0 != chmod(main_bin_path.string().c_str(), S_IRUSR | S_IRGRP | S_IROTH | S_IXUSR | S_IXGRP | S_IXOTH))
                    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot chmod {}", main_bin_path.string());
            }
            else
            {
                if (!fs::exists(bin_dir))
                {
                    fmt::print("Creating binary directory {}.\n", bin_dir.string());
                    fs::create_directories(bin_dir);
                }

                size_t available_space = fs::space(bin_dir).available;
                if (available_space < binary_size)
                    throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space for clickhouse binary in {}, required {}, available {}.",
                        bin_dir.string(), ReadableSize(binary_size), ReadableSize(available_space));

                fmt::print("Copying ClickHouse binary to {}\n", main_bin_tmp_path.string());

                try
                {
                    String source = binary_self_path.string();
                    String destination = main_bin_tmp_path.string();

                    /// Try to make a hard link first, as an optimization.
                    /// It is possible if the source and the destination are on the same filesystems.
                    if (0 != link(source.c_str(), destination.c_str()))
                    {
                        ReadBufferFromFile in(binary_self_path.string());
                        WriteBufferFromFile out(main_bin_tmp_path.string());
                        copyData(in, out);
                        out.sync();
                        out.finalize();
                    }

                    if (0 != chmod(destination.c_str(), S_IRUSR | S_IRGRP | S_IROTH | S_IXUSR | S_IXGRP | S_IXOTH))
                        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot chmod {}", main_bin_tmp_path.string());
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::CANNOT_OPEN_FILE && geteuid() != 0)
                        std::cerr << "Install must be run as root: " << formatWithSudo("./clickhouse install") << '\n';
                    throw;
                }

                if (old_binary_exists)
                {
                    fmt::print("{} already exists, will rename existing binary to {} and put the new binary in place\n",
                            main_bin_path.string(), main_bin_old_path.string());

                    /// There is file exchange operation in Linux but it's not portable.
                    fs::rename(main_bin_path, main_bin_old_path);
                }

                fmt::print("Renaming {} to {}.\n", main_bin_tmp_path.string(), main_bin_path.string());
                fs::rename(main_bin_tmp_path, main_bin_path);
            }
        }

        /// Create symlinks.

        std::initializer_list<std::string_view> tools
        {
            "clickhouse-server",
            "clickhouse-client",
            "clickhouse-local",
            "clickhouse-benchmark",
            "clickhouse-obfuscator",
            "clickhouse-git-import",
            "clickhouse-compressor",
            "clickhouse-format",
            "clickhouse-extract-from-config",
            "clickhouse-keeper",
            "clickhouse-keeper-converter",
            "clickhouse-disks",
#if USE_CHDIG
            "clickhouse-chdig",
            "chdig",
#endif
            "ch",
            "chl",
            "chc",
        };

        for (const auto & tool : tools)
        {
            bool need_to_create = true;
            fs::path symlink_path = bin_dir / tool;

            if (fs::exists(symlink_path))
            {
                /// Do not replace short named symlinks if they are already present in the system
                /// to avoid collision with other tools.
                if (!tool.starts_with("clickhouse"))
                {
                    fmt::print("Symlink {} already exists. Will keep it.\n", symlink_path.string());
                    need_to_create = false;
                }
                else
                {
                    bool is_symlink = FS::isSymlink(symlink_path);
                    fs::path points_to;
                    if (is_symlink)
                        points_to = fs::weakly_canonical(FS::readSymlink(symlink_path));

                    if (is_symlink && (points_to == main_bin_path || (options.count("link") && points_to == binary_self_canonical_path)))
                    {
                        need_to_create = false;
                    }
                    else
                    {
                        if (!is_symlink)
                        {
                            fs::path rename_path = symlink_path.replace_extension(".old");
                            fmt::print("File {} already exists but it's not a symlink. Will rename to {}.\n",
                                       symlink_path.string(), rename_path.string());
                            fs::rename(symlink_path, rename_path);
                        }
                        else if (points_to != main_bin_path)
                        {
                            fmt::print("Symlink {} already exists but it points to {}. Will replace the old symlink to {}.\n",
                                       symlink_path.string(), points_to.string(), main_bin_path.string());
                            (void)fs::remove(symlink_path);
                        }
                    }
                }
            }

            if (need_to_create)
            {
                fmt::print("Creating symlink {} to {}.\n", symlink_path.string(), main_bin_path.string());
                fs::create_symlink(main_bin_path, symlink_path);
            }
        }

        /// Creation of clickhouse user and group.

        std::string user = options["user"].as<std::string>();
        std::string group = options["group"].as<std::string>();

        if (!group.empty())
        {
            fmt::print("Creating clickhouse group if it does not exist.\n");
            createGroup(group);
        }
        else
            fmt::print("Will not create a dedicated clickhouse group.\n");

        if (!user.empty())
        {
            fmt::print("Creating clickhouse user if it does not exist.\n");
            createUser(user, group);

            if (group.empty())
                group = user;

            /// Setting ulimits.
            try
            {
#if defined(OS_DARWIN)

                /// TODO Set ulimits on macOS.

#else
                fs::path ulimits_dir = "/etc/security/limits.d";
                fs::path ulimits_file = ulimits_dir / fmt::format("{}.conf", user);
                fmt::print("Will set ulimits for {} user in {}.\n", user, ulimits_file.string());
                std::string ulimits_content = fmt::format(
                    "{0}\tsoft\tnofile\t1048576\n"
                    "{0}\thard\tnofile\t1048576\n", user);

                fs::create_directories(ulimits_dir);

                WriteBufferFromFile out(ulimits_file.string());
                out.write(ulimits_content.data(), ulimits_content.size());
                out.sync();
                out.finalize();
#endif
            }
            catch (...)
            {
                std::cerr << "Cannot set ulimits: " << getCurrentExceptionMessage(false) << "\n";
            }
        }
        else
            fmt::print("Will not create a dedicated clickhouse user.\n");

        /// Creating configuration files and directories.

        fs::path config_dir = prefix / options["config-path"].as<std::string>();

        if (!fs::exists(config_dir))
        {
            fmt::print("Creating config directory {}.\n", config_dir.string());
            fs::create_directories(config_dir);
        }

        fs::path main_config_file = config_dir / "config.xml";
        fs::path users_config_file = config_dir / "users.xml";
        fs::path config_d = config_dir / "config.d";
        fs::path users_d = config_dir / "users.d";

        fs::path log_path = prefix / options["log-path"].as<std::string>();
        fs::path data_path = prefix / options["data-path"].as<std::string>();
        fs::path pid_path = prefix / options["pid-path"].as<std::string>();

        bool has_password_for_default_user = false;

        if (!fs::exists(config_d))
        {
            fmt::print("Creating config directory {} that is used for tweaks of main server configuration.\n", config_d.string());
            fs::create_directory(config_d);
        }

        if (!fs::exists(users_d))
        {
            fmt::print("Creating config directory {} that is used for tweaks of users configuration.\n", users_d.string());
            fs::create_directory(users_d);
        }

        if (!fs::exists(main_config_file))
        {
            std::string_view main_config_content(reinterpret_cast<const char *>(gresource_config_xmlData), gresource_config_xmlSize);
            if (main_config_content.empty())
            {
                fmt::print("There is no default config.xml, you have to download it and place to {}.\n", main_config_file.string());
            }
            else
            {
                {
                    WriteBufferFromFile out(main_config_file.string());
                    out.write(main_config_content.data(), main_config_content.size());
                    out.sync();
                    out.finalize();
                }

                /// Override the default paths.

                /// Data paths.
                const std::string data_file = config_d / "data-paths.xml";
                if (!fs::exists(data_file))
                {
                    WriteBufferFromFile out(data_file);
                    out << "<clickhouse>\n"
                    "    <path>" << data_path.string() << "</path>\n"
                    "    <tmp_path>" << (data_path / "tmp").string() << "</tmp_path>\n"
                    "    <user_files_path>" << (data_path / "user_files").string() << "</user_files_path>\n"
                    "    <format_schema_path>" << (data_path / "format_schemas").string() << "</format_schema_path>\n"
                    "</clickhouse>\n";
                    out.sync();
                    out.finalize();
                    fs::permissions(data_file, fs::perms::owner_read, fs::perm_options::replace);
                    fmt::print("Data path configuration override is saved to file {}.\n", data_file);
                }

                /// Logger.
                const std::string logger_file = config_d / "logger.xml";
                if (!fs::exists(logger_file))
                {
                    WriteBufferFromFile out(logger_file);
                    out << "<clickhouse>\n"
                    "    <logger>\n"
                    "        <log>" << (log_path / "clickhouse-server.log").string() << "</log>\n"
                    "        <errorlog>" << (log_path / "clickhouse-server.err.log").string() << "</errorlog>\n"
                    "    </logger>\n"
                    "</clickhouse>\n";
                    out.sync();
                    out.finalize();
                    fs::permissions(logger_file, fs::perms::owner_read, fs::perm_options::replace);
                    fmt::print("Log path configuration override is saved to file {}.\n", logger_file);
                }

                /// User directories.
                const std::string user_directories_file = config_d / "user-directories.xml";
                if (!fs::exists(user_directories_file))
                {
                    WriteBufferFromFile out(user_directories_file);
                    out << "<clickhouse>\n"
                    "    <user_directories>\n"
                    "        <local_directory>\n"
                    "            <path>" << (data_path / "access").string() << "</path>\n"
                    "        </local_directory>\n"
                    "    </user_directories>\n"
                    "</clickhouse>\n";
                    out.sync();
                    out.finalize();
                    fs::permissions(user_directories_file, fs::perms::owner_read, fs::perm_options::replace);
                    fmt::print("User directory path configuration override is saved to file {}.\n", user_directories_file);
                }

                /// OpenSSL.
                const std::string openssl_file = config_d / "openssl.xml";
                if (!fs::exists(openssl_file))
                {
                    WriteBufferFromFile out(openssl_file);
                    out << "<clickhouse>\n"
                    "    <openSSL>\n"
                    "        <server>\n"
                    "            <certificateFile>" << (config_dir / "server.crt").string() << "</certificateFile>\n"
                    "            <privateKeyFile>" << (config_dir / "server.key").string() << "</privateKeyFile>\n"
                    "        </server>\n"
                    "    </openSSL>\n"
                    "</clickhouse>\n";
                    out.sync();
                    out.finalize();
                    fs::permissions(openssl_file, fs::perms::owner_read, fs::perm_options::replace);
                    fmt::print("OpenSSL path configuration override is saved to file {}.\n", openssl_file);
                }
            }
        }
        else
        {
            fmt::print("Config file {} already exists, will keep it and extract path info from it.\n", main_config_file.string());

            ConfigProcessor processor(main_config_file.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
            ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(processor.processConfig()));

            if (configuration->has("path"))
            {
                data_path = configuration->getString("path");
                fmt::print("{} has {} as data path.\n", main_config_file.string(), data_path.string());
            }

            if (configuration->has("logger.log"))
            {
                log_path = fs::path(configuration->getString("logger.log")).remove_filename();
                fmt::print("{} has {} as log path.\n", main_config_file.string(), log_path.string());
            }
        }


        if (!fs::exists(users_config_file))
        {
            std::string_view users_config_content(reinterpret_cast<const char *>(gresource_users_xmlData), gresource_users_xmlSize);
            if (users_config_content.empty())
            {
                fmt::print("There is no default users.xml, you have to download it and place to {}.\n", users_config_file.string());
            }
            else
            {
                WriteBufferFromFile out(users_config_file.string());
                out.write(users_config_content.data(), users_config_content.size());
                out.sync();
                out.finalize();
            }
        }
        else
        {
            fmt::print("Users config file {} already exists, will keep it and extract users info from it.\n", users_config_file.string());

            /// Check if password for the default user already specified.
            ConfigProcessor processor(users_config_file.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
            ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(processor.processConfig()));

            if (!configuration->getString("users.default.password", "").empty()
                || !configuration->getString("users.default.password_sha256_hex", "").empty()
                || !configuration->getString("users.default.password_double_sha1_hex", "").empty())
            {
                has_password_for_default_user = true;
            }
        }

        /// Don't allow relative paths because install script may cd to / when installing
        /// And having path=./ may break the system
        if (log_path.is_relative())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Log path is relative: {}", log_path.string());
        if (data_path.is_relative())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Data path is relative: {}", data_path.string());
        if (pid_path.is_relative())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pid path is relative: {}", pid_path.string());

        /// Create directories for data and log.

        if (fs::exists(log_path))
        {
            fmt::print("Log directory {} already exists.\n", log_path.string());
        }
        else
        {
            fmt::print("Creating log directory {}.\n", log_path.string());
            fs::create_directories(log_path);
        }

        if (fs::exists(data_path))
        {
            fmt::print("Data directory {} already exists.\n", data_path.string());
        }
        else
        {
            fmt::print("Creating data directory {}.\n", data_path.string());
            fs::create_directories(data_path);
        }

        if (fs::exists(pid_path))
        {
            fmt::print("Pid directory {} already exists.\n", pid_path.string());
        }
        else
        {
            fmt::print("Creating pid directory {}.\n", pid_path.string());
            fs::create_directories(pid_path);
        }

        /// Chmod and chown data and log directories
        changeOwnership(log_path, user, group);
        changeOwnership(pid_path, user, group);

        /// Not recursive, because there can be a huge number of files and it will be slow.
        changeOwnership(data_path, user, group, /* recursive= */ false);

        /// All users are allowed to read pid file (for clickhouse status command).
        fs::permissions(pid_path, fs::perms::owner_all | fs::perms::group_read | fs::perms::others_read, fs::perm_options::replace);

        /// Other users in clickhouse group are allowed to read and even delete logs.
        fs::permissions(log_path, fs::perms::owner_all | fs::perms::group_all, fs::perm_options::replace);

        /// Data directory is not accessible to anyone except clickhouse.
        fs::permissions(data_path, fs::perms::owner_all, fs::perm_options::replace);

        fs::path odbc_bridge_path = bin_dir / "clickhouse-odbc-bridge";
        fs::path library_bridge_path = bin_dir / "clickhouse-library-bridge";

        if (fs::exists(odbc_bridge_path) || fs::exists(library_bridge_path))
        {
            createGroup(DEFAULT_CLICKHOUSE_BRIDGE_GROUP);
            createUser(DEFAULT_CLICKHOUSE_BRIDGE_USER, DEFAULT_CLICKHOUSE_BRIDGE_GROUP);

            if (fs::exists(odbc_bridge_path))
                changeOwnership(odbc_bridge_path, DEFAULT_CLICKHOUSE_BRIDGE_USER, DEFAULT_CLICKHOUSE_BRIDGE_GROUP);
            if (fs::exists(library_bridge_path))
                changeOwnership(library_bridge_path, DEFAULT_CLICKHOUSE_BRIDGE_USER, DEFAULT_CLICKHOUSE_BRIDGE_GROUP);
        }

        bool stdin_is_a_tty = isatty(STDIN_FILENO);
        bool stdout_is_a_tty = isatty(STDOUT_FILENO);

        /// dpkg or apt installers can ask for non-interactive work explicitly.

        const char * debian_frontend_var = getenv("DEBIAN_FRONTEND"); // NOLINT(concurrency-mt-unsafe)
        bool noninteractive = (debian_frontend_var && debian_frontend_var == std::string_view("noninteractive"))
                              || options.count("noninteractive");


        bool is_interactive = !noninteractive && stdin_is_a_tty && stdout_is_a_tty;

        /// We can ask password even if stdin is closed/redirected but /dev/tty is available.
        bool can_ask_password = !noninteractive && stdout_is_a_tty;

        /// Set up password for default user.
        if (has_password_for_default_user)
        {
            fmt::print("{}Password for the default user is already specified. To remind or reset, see {} and {}.{}\n",
                start_hilite, users_config_file.string(), users_d.string(), end_hilite);
        }
        else if (!can_ask_password)
        {
            fmt::print("{}Password for the default user is an empty string. See {} and {} to change it.{}\n",
                start_hilite, users_config_file.string(), users_d.string(), end_hilite);
        }
        else
        {
            /// NOTE: When installing debian package with dpkg -i, stdin is not a terminal but we are still being able to enter password.
            /// More sophisticated method with /dev/tty is used inside the `readpassphrase` function.

            char buf[1000] = {};
            std::string password;
            if (auto * result = readpassphrase("Set up the password for the default user: ", buf, sizeof(buf), 0))
                password = result;

            if (!password.empty())
            {
                std::string password_file = users_d / "default-password.xml";
                WriteBufferFromFile out(password_file);
#if USE_SSL
                std::vector<uint8_t> hash;
                hash.resize(32);
                encodeSHA256(password, hash.data());
                std::string hash_hex;
                hash_hex.resize(64);
                for (size_t i = 0; i < 32; ++i)
                    writeHexByteLowercase(hash[i], &hash_hex[2 * i]);
                out << "<clickhouse>\n"
                    "    <users>\n"
                    "        <default>\n"
                    "            <password remove='1' />\n"
                    "            <password_sha256_hex>" << hash_hex << "</password_sha256_hex>\n"
                    "        </default>\n"
                    "    </users>\n"
                    "</clickhouse>\n";
                out.sync();
                out.finalize();
                fmt::print("{}Password for the default user is saved in file {}.{}\n", start_hilite, password_file, end_hilite);
#else
                out << "<clickhouse>\n"
                    "    <users>\n"
                    "        <default>\n"
                    "            <password><![CDATA[" << password << "]]></password>\n"
                    "        </default>\n"
                    "    </users>\n"
                    "</clickhouse>\n";
                out.sync();
                out.finalize();
                fmt::print("{}Password for the default user is saved in plaintext in file {}.{}\n", start_hilite, password_file, end_hilite);
#endif
                has_password_for_default_user = true;
            }
            else
                fmt::print("{}Password for the default user is an empty string. See {} and {} to change it.{}\n",
                    start_hilite, users_config_file.string(), users_d.string(), end_hilite);
        }

        /** Set capabilities for the binary.
          *
          * 1. Check that "setcap" tool exists.
          * 2. Check that an arbitrary program with installed capabilities can run.
          * 3. Set the capabilities.
          *
          * The second is important for Docker and systemd-nspawn.
          * When the container has no capabilities,
          * but the executable file inside the container has capabilities,
          *  then attempt to run this file will end up with a cryptic "Operation not permitted" message.
          */

#if defined(OS_LINUX)
        fmt::print("Setting capabilities for clickhouse binary. This is optional.\n");
        std::string command = fmt::format("command -v setcap >/dev/null"
            " && command -v capsh >/dev/null"
            " && capsh --has-p=cap_net_admin,cap_ipc_lock,cap_sys_nice,cap_net_bind_service+ep >/dev/null 2>&1"
            " && setcap 'cap_net_admin,cap_ipc_lock,cap_sys_nice,cap_net_bind_service+ep' {0}"
            " || echo \"Cannot set 'net_admin' or 'ipc_lock' or 'sys_nice' or 'net_bind_service' capability for clickhouse binary."
                " This is optional. Taskstats accounting will be disabled."
                " To enable taskstats accounting you may add the required capability later manually.\"",
            fs::canonical(main_bin_path).string());
        executeScript(command);
#endif

        /// If password was set, ask for open for connections.
        if (is_interactive && has_password_for_default_user)
        {
            if (ask("Allow server to accept connections from the network (default is localhost only), [y/N]: "))
            {
                std::string listen_file = config_d / "listen.xml";
                WriteBufferFromFile out(listen_file);
                out << "<clickhouse>\n"
                    "    <listen_host>::</listen_host>\n"
                    "</clickhouse>\n";
                out.sync();
                out.finalize();
                fmt::print("The choice is saved in file {}.\n", listen_file);
            }
        }

        /// Chmod and chown configs
        changeOwnership(config_dir, user, group);

        /// Symlink "preprocessed_configs" is created by the server, so "write" is needed.
        fs::permissions(config_dir, fs::perms::owner_all, fs::perm_options::replace);

        /// Subdirectories, so "execute" is needed.
        if (fs::exists(config_d))
            fs::permissions(config_d, fs::perms::owner_read | fs::perms::owner_exec, fs::perm_options::replace);
        if (fs::exists(users_d))
            fs::permissions(users_d, fs::perms::owner_read | fs::perms::owner_exec, fs::perm_options::replace);

        /// Readonly.
        if (fs::exists(main_config_file))
            fs::permissions(main_config_file, fs::perms::owner_read, fs::perm_options::replace);
        if (fs::exists(users_config_file))
            fs::permissions(users_config_file, fs::perms::owner_read, fs::perm_options::replace);


        std::string maybe_password;
        if (has_password_for_default_user)
            maybe_password = " --password";

        fs::path pid_file = pid_path / "clickhouse-server.pid";
        if (fs::exists(pid_file))
        {
            fmt::print(
                "\nClickHouse has been successfully installed.\n"
                "\nRestart clickhouse-server with:\n"
                " {}\n"
                "\nStart clickhouse-client with:\n"
                " clickhouse-client{}\n\n",
                formatWithSudo("clickhouse restart"),
                maybe_password);
        }
        else
        {
            fmt::print(
                "\nClickHouse has been successfully installed.\n"
                "\nStart clickhouse-server with:\n"
                " {}\n"
                "\nStart clickhouse-client with:\n"
                " clickhouse-client{}\n\n",
                formatWithSudo("clickhouse start"),
                maybe_password);
        }
    }
    catch (const fs::filesystem_error &)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';

        if (getuid() != 0)
            std::cerr << "\nRun with " << formatWithSudo("...") << "\n";

        return getCurrentExceptionCode();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}


namespace
{
    int start(const std::string & user, const fs::path & executable, const fs::path & config, const fs::path & pid_file, unsigned max_tries)
    {
        if (fs::exists(pid_file))
        {
            ReadBufferFromFile in(pid_file.string());
            Int32 pid;
            if (tryReadIntText(pid, in))
            {
                fmt::print("{} file exists and contains pid = {}.\n", pid_file.string(), pid);

                if (0 == kill(pid, 0))
                {
                    fmt::print("The process with pid = {} is already running.\n", pid);
                    return 2;
                }
            }
            else
            {
                fmt::print("{} file exists but damaged, ignoring.\n", pid_file.string());
                (void)fs::remove(pid_file);
            }
        }
        else
        {
            /// Create a directory for pid file.
            /// It's created by "install" but we also support cases when ClickHouse is already installed different way.
            fs::path pid_path = pid_file;
            pid_path = pid_path.remove_filename();
            fs::create_directories(pid_path);
            /// All users are allowed to read pid file (for clickhouse status command).
            fs::permissions(pid_path, fs::perms::owner_all | fs::perms::group_read | fs::perms::others_read, fs::perm_options::replace);

            changeOwnership(pid_path, user);
        }

        std::string command = fmt::format("{} --config-file {} --pid-file {} --daemon",
            executable.string(), config.string(), pid_file.string());

        if (!user.empty())
        {
            /// sudo respects limits in /etc/security/limits.conf e.g. open files,
            /// that's why we are using it instead of the 'clickhouse su' tool.
            /// by default, sudo resets all the ENV variables, but we should preserve
            /// the values /etc/default/clickhouse in /etc/init.d/clickhouse file
            command = fmt::format("sudo --preserve-env -u '{}' {}", user, command);
        }

        fmt::print("Will run {}\n", command);
        executeScript(command, true);

        /// Wait to start.

        size_t try_num = 0;
        for (; try_num < max_tries; ++try_num)
        {
            fmt::print("Waiting for server to start\n");
            if (fs::exists(pid_file))
            {
                fmt::print("Server started\n");
                break;
            }
            sleepForSeconds(1);
        }

        if (try_num == max_tries)
        {
            fmt::print("Cannot start server. You can execute {} without --daemon option to run manually.\n", command);

            fs::path log_path;

            {
                ConfigProcessor processor(config.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
                ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(processor.processConfig()));

                if (configuration->has("logger.log"))
                    log_path = fs::path(configuration->getString("logger.log")).remove_filename();
            }

            if (log_path.empty())
            {
                fmt::print("Cannot obtain path to logs (logger.log) from config file {}.\n", config.string());
            }
            else
            {
                fs::path stderr_path = log_path;
                stderr_path.replace_filename("stderr.log");
                fmt::print("Look for logs at {} and for {}.\n", log_path.string(), stderr_path.string());
            }

            return 3;
        }

        return 0;
    }

    int isRunning(const fs::path & pid_file, bool ignore_file_does_not_exist)
    {
        int pid = 0;

        try
        {
            ReadBufferFromFile in(pid_file.string());
            if (tryReadIntText(pid, in))
            {
                fmt::print("{} file exists and contains pid = {}.\n", pid_file.string(), pid);
            }
            else
            {
                fmt::print("{} file exists but damaged, ignoring (the file will be removed).\n", pid_file.string());
                (void)fs::remove(pid_file);
            }
        }
        catch (const Exception & e)
        {
            if (!ignore_file_does_not_exist || e.code() != ErrorCodes::FILE_DOESNT_EXIST)
                throw;
        }

        if (pid)
        {
            if (0 == kill(pid, 0))
            {
                fmt::print("The process with pid = {} is running.\n", pid);
            }
            else if (errno == ESRCH)
            {
                fmt::print("The process with pid = {} does not exist.\n", pid);
                return 0;
            }
            else
                throw ErrnoException(ErrorCodes::CANNOT_KILL, "Cannot obtain the status of pid {} with `kill`", pid);
        }

        if (!pid)
        {
            fmt::print("Now there is no clickhouse-server process.\n");
        }

        return pid;
    }

    bool sendSignalAndWaitForStop(const fs::path & pid_file, int signal, unsigned max_tries, unsigned wait_ms, const char * signal_name)
    {
        int pid = isRunning(pid_file, /*ignore_file_does_not_exist=*/ false);

        if (!pid)
            return true;

        if (0 == kill(pid, signal))
            fmt::print("Sent {} signal to process with pid {}.\n", signal_name, pid);
        else
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot send {} signal", signal_name);

        size_t try_num = 0;
        for (; try_num < max_tries; ++try_num)
        {
            fmt::print("Waiting for server to stop\n");
            if (!isRunning(pid_file, /*ignore_file_does_not_exist=*/ true))
            {
                fmt::print("Server stopped\n");
                break;
            }
            sleepForMilliseconds(wait_ms);
        }

        return try_num < max_tries;
    }

    int stop(const fs::path & pid_file, bool force, bool do_not_kill, unsigned max_tries)
    {
        if (force && do_not_kill)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Specified flags are incompatible");

        int signal = force ? SIGKILL : SIGTERM;
        const char * signal_name = force ? "kill" : "terminate";

        if (sendSignalAndWaitForStop(pid_file, signal, max_tries, 1000, signal_name))
            return 0;

        int pid = isRunning(pid_file, /*ignore_file_does_not_exist=*/ false);
        if (!pid)
            return 0;

        if (do_not_kill)
        {
            fmt::print("Process (pid = {}) is still running. Will not try to kill it.\n", pid);
            return 1;
        }

        /// Send termination signal again, the server will receive it and immediately terminate.
        fmt::print("Will send the termination signal again to force the termination (pid = {}).\n", pid);
        if (sendSignalAndWaitForStop(pid_file, signal, std::min(10U, max_tries), 1000, signal_name))
            return 0;

        /// Send kill signal. Total wait is 100 seconds.
        constexpr size_t num_kill_check_tries = 1000;
        constexpr size_t kill_check_delay_ms = 100;
        fmt::print("Will terminate forcefully (pid = {}).\n", pid);
        if (sendSignalAndWaitForStop(pid_file, SIGKILL, num_kill_check_tries, kill_check_delay_ms, "kill"))
            return 0;

        if (!isRunning(pid_file, /*ignore_file_does_not_exist=*/ true))
            return 0;

        throw Exception(ErrorCodes::CANNOT_KILL,
            "The server process still exists after {} tries (delay: {} ms)",
            num_kill_check_tries, kill_check_delay_ms);
    }
}


int mainEntryClickHouseStart(int argc, char ** argv)
{
    try
    {
        po::options_description desc;
        desc.add_options()
            ("help,h", "produce help message")
            ("prefix", po::value<std::string>()->default_value("/"), "prefix for all paths")
#if defined (OS_DARWIN)
            /// https://stackoverflow.com/a/36734569/22422288
            ("binary-path", po::value<std::string>()->default_value("usr/local/bin"), "directory with binary")
#else
            ("binary-path", po::value<std::string>()->default_value("usr/bin"), "directory with binary")
#endif
            ("config-path", po::value<std::string>()->default_value("etc/clickhouse-server"), "directory with configs")
            ("pid-path", po::value<std::string>()->default_value("var/run/clickhouse-server"), "directory for pid file")
            ("user", po::value<std::string>()->default_value(DEFAULT_CLICKHOUSE_SERVER_USER), "clickhouse user")
            ("max-tries", po::value<unsigned>()->default_value(60), "Max number of tries for waiting the server (with 1 second delay)")
        ;

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << formatWithSudo(std::string(argv[0]) + " start", getuid() != 0) << '\n';
            return 1;
        }

        std::string user = options["user"].as<std::string>();

        fs::path prefix = options["prefix"].as<std::string>();
        fs::path executable = prefix / options["binary-path"].as<std::string>() / "clickhouse-server";
        fs::path config = prefix / options["config-path"].as<std::string>() / "config.xml";
        fs::path pid_file = prefix / options["pid-path"].as<std::string>() / "clickhouse-server.pid";
        unsigned max_tries = options["max-tries"].as<unsigned>();

        return start(user, executable, config, pid_file, max_tries);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}


int mainEntryClickHouseStop(int argc, char ** argv)
{
    try
    {
        po::options_description desc;
        desc.add_options()
            ("help,h", "produce help message")
            ("prefix", po::value<std::string>()->default_value("/"), "prefix for all paths")
            ("pid-path", po::value<std::string>()->default_value("var/run/clickhouse-server"), "directory for pid file")
            ("force", po::bool_switch(), "Stop with KILL signal instead of TERM")
            ("do-not-kill", po::bool_switch(), "Do not send KILL even if TERM did not help")
            ("max-tries", po::value<unsigned>()->default_value(60), "Max number of tries for waiting the server to finish after sending TERM (with 1 second delay)")
        ;

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << formatWithSudo(std::string(argv[0]) + " stop", getuid() != 0) << '\n';
            return 1;
        }

        fs::path prefix = options["prefix"].as<std::string>();
        fs::path pid_file = prefix / options["pid-path"].as<std::string>() / "clickhouse-server.pid";

        bool force = options["force"].as<bool>();
        bool do_not_kill = options["do-not-kill"].as<bool>();
        unsigned max_tries = options["max-tries"].as<unsigned>();
        return stop(pid_file, force, do_not_kill, max_tries);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}


int mainEntryClickHouseStatus(int argc, char ** argv)
{
    try
    {
        po::options_description desc;
        desc.add_options()
            ("help,h", "produce help message")
            ("prefix", po::value<std::string>()->default_value("/"), "prefix for all paths")
            ("pid-path", po::value<std::string>()->default_value("var/run/clickhouse-server"), "directory for pid file")
        ;

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << formatWithSudo(std::string(argv[0]) + " status", getuid() != 0) << '\n';
            return 1;
        }

        fs::path prefix = options["prefix"].as<std::string>();
        fs::path pid_file = prefix / options["pid-path"].as<std::string>() / "clickhouse-server.pid";

        isRunning(pid_file, /*ignore_file_does_not_exist=*/ false);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}


int mainEntryClickHouseRestart(int argc, char ** argv)
{
    try
    {
        po::options_description desc;
        desc.add_options()
            ("help,h", "produce help message")
            ("prefix", po::value<std::string>()->default_value("/"), "prefix for all paths")
#if defined (OS_DARWIN)
            /// https://stackoverflow.com/a/36734569/22422288
            ("binary-path", po::value<std::string>()->default_value("usr/local/bin"), "directory with binary")
#else
            ("binary-path", po::value<std::string>()->default_value("usr/bin"), "directory with binary")
#endif
            ("config-path", po::value<std::string>()->default_value("etc/clickhouse-server"), "directory with configs")
            ("pid-path", po::value<std::string>()->default_value("var/run/clickhouse-server"), "directory for pid file")
            ("user", po::value<std::string>()->default_value(DEFAULT_CLICKHOUSE_SERVER_USER), "clickhouse user")
            ("force", po::value<bool>()->default_value(false), "Stop with KILL signal instead of TERM")
            ("do-not-kill", po::bool_switch(), "Do not send KILL even if TERM did not help")
            ("max-tries", po::value<unsigned>()->default_value(60), "Max number of tries for waiting the server (with 1 second delay)")
        ;

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << formatWithSudo(std::string(argv[0]) + " restart", getuid() != 0) << '\n';
            return 1;
        }

        std::string user = options["user"].as<std::string>();

        fs::path prefix = options["prefix"].as<std::string>();
        fs::path executable = prefix / options["binary-path"].as<std::string>() / "clickhouse-server";
        fs::path config = prefix / options["config-path"].as<std::string>() / "config.xml";
        fs::path pid_file = prefix / options["pid-path"].as<std::string>() / "clickhouse-server.pid";

        bool force = options["force"].as<bool>();
        bool do_not_kill = options["do-not-kill"].as<bool>();
        unsigned max_tries = options["max-tries"].as<unsigned>();

        if (int res = stop(pid_file, force, do_not_kill, max_tries))
            return res;
        return start(user, executable, config, pid_file, max_tries);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}
