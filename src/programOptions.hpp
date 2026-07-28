#ifndef PROGRAM_OPTIONS_HPP
#define PROGRAM_OPTIONS_HPP
#include <filesystem>
#include <iostream>
#include <optional>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "uDataPacketBroker/brokerOptions.hpp"
#include "uDataPacketBroker/grpcServerOptions.hpp"
#include "uDataPacketBroker/publishServiceOptions.hpp"
#include "uDataPacketBroker/rocksDatabaseOptions.hpp"
#include "uDataPacketBroker/subscribeServiceOptions.hpp"
#include "otelOptions.hpp"
namespace 
{

#define APPLICATION_NAME "uDataPacketBroker"

struct ProgramOptions
{
    UDataPacketBroker::OTelOptions::HTTPMetrics otelHTTPMetricsOptions;
    UDataPacketBroker::OTelOptions::HTTPLog otelHTTPLogOptions;
    UDataPacketBroker::OTelOptions::GRPCMetrics otelGRPCMetricsOptions;
    UDataPacketBroker::OTelOptions::GRPCLog otelGRPCLogOptions;
    UDataPacketBroker::BrokerOptions brokerOptions;
    std::optional<UDataPacketBroker::RocksDatabaseOptions>
        rocksDatabaseOptions{std::nullopt};
    std::string applicationName{APPLICATION_NAME};
    std::chrono::seconds printSummaryInterval{std::chrono::minutes {15}};
    int64_t maximumImportQueueSize{8192};
    int verbosity{3};
    bool exportLogs{false};
    bool exportLogsWithHTTP{true};
    bool exportMetrics{false};
    bool exportMetricsWithHTTP{true};
};

std::pair<std::filesystem::path, bool>
    parseCommandLineOptions(int argc, char *argv[])
{
    std::filesystem::path iniFile;
    boost::program_options::options_description desc(R"""(
The uDataPacketBroker is pub/sub middleware backed by a store.  It receives
input packets from various data sources and forwards thoes packets to 
interested parties.

    uDataPacketBroker --ini=broker.ini

Allowed options)""");
    desc.add_options()
        ("help", "Produces this help message")
        ("ini",  boost::program_options::value<std::string> (), 
                 "The initialization file for this executable");
    boost::program_options::variables_map vm; 
    //NOLINTBEGIN(misc-include-cleaner)
    auto parsedMap
        = boost::program_options::parse_command_line(argc, argv, desc);
    //NOLINTEND(misc-include-cleaner)
    boost::program_options::store(parsedMap, vm);
    boost::program_options::notify(vm);
    if (vm.count("help"))
    {   
        std::cout << desc << "\n";
        return {iniFile, true};
    }   
    if (vm.count("ini"))
    {   
        iniFile = vm["ini"].as<std::string> (); 
        if (!std::filesystem::exists(iniFile))
        {
            throw std::runtime_error("Initialization file: "
                                   + std::string {iniFile}
                                   + " does not exist");
        }
    }
    return {iniFile, false};
}

std::optional<UDataPacketBroker::RocksDatabaseOptions> getRocksDBOptions(
    boost::property_tree::ptree &propertyTree,
    const std::string &section = "RocksDB")
{
    using namespace UDataPacketBroker;
    if (!propertyTree.get_optional<std::string> ("RocksDB"))
    {
        return std::nullopt;
    }
    RocksDatabaseOptions options;

    // Directory
    auto databaseDirectory
        = propertyTree.get<std::string> (section + ".directory", "rocksdb");
    if (databaseDirectory.empty()){databaseDirectory = "./";}
    if (!std::filesystem::exists(databaseDirectory))
    {
        if (!std::filesystem::create_directories(databaseDirectory))
        {
            throw std::runtime_error("Could not make RocksDB directory "
                                   + databaseDirectory);
        }
    }
    options.setDatabaseDirectory(databaseDirectory);

    // Overwrite the database if it exists
    auto overWriteIfExists
        = propertyTree.get<bool> (section + ".overWriteIfExists", false);
    options.disableOverWriteIfExists();
    if (overWriteIfExists){options.enableOverWriteIfExists();}

    // Use WAL
    auto enableWAL
        = propertyTree.get<bool> (section + ".enableWriteAheadLog", false);
    options.disableWriteAheadLog();
    if (enableWAL){options.enableWriteAheadLog();}

    // Write buffer size in bytes
    auto writeBufferSizeInBytes
        = propertyTree.get<size_t> (section + ".writeBufferSizeInBytes", 
                                    options.getWriteBufferSizeInBytes());
    if (writeBufferSizeInBytes < 1)
    {
        throw std::invalid_argument("Write buffer size must be positive");
    }
    options.setWriteBufferSizeInBytes(writeBufferSizeInBytes);

    auto maxMemoryTables
       = propertyTree.get<int> (section + ".maximumNumberOfMemoryTables",
                                options.getMaximumNumberOfMemoryTables());
    if (maxMemoryTables < 1)
    {
        throw std::invalid_argument(
           "Max number of memory tables must be positive");
    }
    options.setMaximumNumberOfMemoryTables(maxMemoryTables);

    // N.B. skip retention - that will come from broker details

    return std::make_optional<RocksDatabaseOptions> (options);
}

ProgramOptions parseIniFile(const std::filesystem::path &iniFile)
{
    ProgramOptions options;
    if (!std::filesystem::exists(iniFile)){return options;}
    // Parse the initialization file
    boost::property_tree::ptree propertyTree;
    boost::property_tree::ini_parser::read_ini(iniFile, propertyTree);

    // Application name
    options.applicationName
        = propertyTree.get<std::string> ("General.applicationName",
                                         options.applicationName);
    if (options.applicationName.empty())
    {
        options.applicationName = APPLICATION_NAME;
    }   
    options.verbosity
        = propertyTree.get<int> ("General.verbosity", options.verbosity);

    auto summaryIntervalInMinutes
        = static_cast<int> (options.printSummaryInterval.count());
    summaryIntervalInMinutes
        = propertyTree.get<int> ("General.printSummaryIntervalInMinutes",
                                 summaryIntervalInMinutes);
    options.printSummaryInterval
        = std::chrono::minutes {summaryIntervalInMinutes};

    // Get the RocksDB options
    options.rocksDatabaseOptions = ::getRocksDBOptions(propertyTree, "RocksDB");
    if (options.rocksDatabaseOptions == std::nullopt)
    {
        throw std::runtime_error("Only RocksDB data store is implemented");
    }

    // Get OTel logs options
    auto httpLog
        = UDataPacketBroker::OTelOptions::getHTTPLogOptionsFromIniFile(
             propertyTree,
             "OTelHTTPLogOptions");
    options.exportLogs = false;
    if (httpLog != std::nullopt)
    {
        options.otelHTTPLogOptions = *httpLog;
        options.exportLogs = true; 
        options.exportLogsWithHTTP = true;
    }
    else
    {
        auto grpcLog
            = UDataPacketBroker::OTelOptions::getGRPCLogOptionsFromIniFile(
                propertyTree,
                "OTelGRPCLogOptions");
        if (grpcLog != std::nullopt)
        {
            options.otelGRPCLogOptions = *grpcLog;
            options.exportLogs = true;
            options.exportLogsWithHTTP = false;
        }
    }

    // Get OTel metrics options
    auto httpMetrics
        = UDataPacketBroker::OTelOptions::getHTTPMetricsOptionsFromIniFile(
             propertyTree,
             "OTelHTTPMetricsOptions");
    options.exportMetrics = false;
    if (httpMetrics != std::nullopt)
    {   
        options.otelHTTPMetricsOptions = *httpMetrics;
        options.exportMetrics = true; 
        options.exportMetricsWithHTTP = true;
    }   
    else
    {   
        auto grpcMetrics
            = UDataPacketBroker::OTelOptions::getGRPCMetricsOptionsFromIniFile(
                propertyTree,
                "OTelGRPCMetricsOptions");
        if (grpcMetrics != std::nullopt)
        {
            options.otelGRPCMetricsOptions = *grpcMetrics;
            options.exportMetrics = true;
            options.exportMetricsWithHTTP = false;
        }
    }   




    return options;
}

}
#endif
