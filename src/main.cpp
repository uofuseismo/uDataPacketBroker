#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stdlib.h> // setenv
#include <string>
#include <thread>
#include <utility>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include "uDataPacketBroker/broker.hpp"
#include "uDataPacketBroker/dataPacketStore.hpp"
#include "uDataPacketBroker/metricsSingleton.hpp"
#include "uDataPacketBroker/rocksDatabase.hpp"
//#include "uDataPacketBroker/rocksDatabaseOptions.hpp"
#include "uDataPacketBroker/version.hpp"
#include "programOptions.hpp"
#include "logger.hpp"
#include "metrics.hpp"

namespace
{

volatile std::sig_atomic_t mSignalStatus;
std::atomic_bool mInterrupted{false};

class Process
{
public:
    Process(ProgramOptions &&options,
            std::shared_ptr<spdlog::logger> logger) :
        mOptions(std::move(options)),
        mLogger(std::move(logger))
    {
#ifndef NDEBUG
        assert(mLogger != nullptr);
#endif
        // Create the backend store
        if (mOptions.rocksDatabaseOptions != std::nullopt)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Creating RocksDB store");
            mDataPacketStore
                = std::make_shared<UDataPacketBroker::RocksDatabase>
                  (*mOptions.rocksDatabaseOptions, mLogger);
        }
        else
        {
            throw std::invalid_argument("No data store defined");
        }
#ifndef NDEBUG
        assert(mDataPacketStore != nullptr);
#endif
        // Now create broker

    }

    /// @brief Destructor
    ~Process()
    {
        stop();
    }

    /// @brief Starts the application.
    void start()
    {
        SPDLOG_LOGGER_INFO(mLogger, "Starting broker");
        mIsRunning = true;
        mBroker->start();
        handleMainThread();
    }

    /// @brief Shuts down the application.
    void stop()
    {
        if (mBroker)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Stopping the broker");
            //mBroker->stop();
            mBroker = nullptr;
        }
    }

    /// @result True indicates the futures are a-okay.
    [[nodiscard]] bool checkFuturesOkay(const std::chrono::milliseconds &timeOut) const
    {
        bool isOkay{true};
        if (mBroker)
        {
            isOkay = mBroker->checkFuturesOkay(timeOut);
        }
        return isOkay;
    }

    /// @brief Periodically prints a summary to a log file as another way
    ///        to demonstrate this application is alive and doing what it is
    ///        supposed to.
    void printSummary()
    {
        /// Summary 
        if (mOptions.printSummaryInterval.count() <= 0){return;}
        const auto now =
            std::chrono::duration_cast<std::chrono::seconds>
            ((std::chrono::high_resolution_clock::now()).time_since_epoch());
        if (now < mLastReport + mOptions.printSummaryInterval){return;}
    }

    /// @brief Handles main thread activities.
    void handleMainThread()
    {
        SPDLOG_LOGGER_INFO(mLogger, "Main thread entering waiting loop");
        catchSignals();
        while (!mStopRequested)
        {
            if (mInterrupted)
            {
                SPDLOG_LOGGER_INFO(mLogger,
                                   "SIGINT/SIGTERM signal received!");
                mStopRequested = true;
                break;
            }
            constexpr std::chrono::milliseconds waitForFuture {5};
            if (!checkFuturesOkay(waitForFuture))
            {
                SPDLOG_LOGGER_CRITICAL(mLogger,
                   "Futures exception caught; terminating app");
                mStopRequested = true;
                break;
            }
            printSummary();
            std::unique_lock<std::mutex> lock(mStopMutex);
            constexpr std::chrono::milliseconds pause{100};
            mStopCondition.wait_for(lock, pause,
                                    [this]
                                    {
                                        return mStopRequested;
                                    });
        }
        if (mStopRequested)
        {
            SPDLOG_LOGGER_DEBUG(mLogger,
                                "Stop request received.  Terminating...");
            stop();
            std::this_thread::sleep_for(std::chrono::milliseconds {15});
        }
    }

    void catchSignals()
    {
        std::signal(SIGINT,  Process::signalHandler);
        std::signal(SIGTERM, Process::signalHandler);
    }

    static void signalHandler(const int signal)
    {
        mSignalStatus = signal;
        mInterrupted.store(true);
    }


//private:
    ::ProgramOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::mutex mStopMutex;
    std::condition_variable mStopCondition;
    std::chrono::seconds mLastReport
    {
        std::chrono::duration_cast<std::chrono::seconds>
        ((std::chrono::high_resolution_clock::now()).time_since_epoch())
    };
    //std::map<std::string, std::future<void>> mFuturesMap;
    std::shared_ptr<UDataPacketBroker::IDataPacketStore>
         mDataPacketStore{nullptr};
    std::unique_ptr<UDataPacketBroker::Broker> mBroker{nullptr};
    bool mStopRequested{false};
    bool mIsRunning{false};
};

}

int main(int argc, char *argv[])
{
    // Initialize metrics up front
    UDataPacketBroker::initializeMetricsSingleton();
    // Create a console logger to deal with startup stuff
    //NOLINTNEXTLINE(misc-include-cleaner)
    auto consoleLogger = spdlog::stdout_color_st("console");
    SPDLOG_LOGGER_INFO(consoleLogger,
                       "Running version {} of uDataPacketBroker",
                       UDataPacketBroker::Version::getVersionWithTag());

    // Parse the command line arguments
    std::filesystem::path iniFile;
    try
    {
        auto [iniFileName, isHelp] = ::parseCommandLineOptions(argc, argv);
        if (isHelp){return EXIT_SUCCESS;}
        if (iniFileName.empty())
        {
            throw std::runtime_error("No initialization file specified");
        }
        iniFile = iniFileName;
    }
    catch (const std::exception &e) 
    {
        SPDLOG_LOGGER_CRITICAL(consoleLogger,
                               "Failed to read command line options because {}",
                               std::string {e.what()});
        return EXIT_FAILURE;
    }

    // Read the program options from the ini file
    ::ProgramOptions programOptions;
    try 
    {   
        programOptions = ::parseIniFile(iniFile);
    }   
    catch (const std::exception &e) 
    {   
        SPDLOG_LOGGER_CRITICAL(consoleLogger,
                               "Failed to read program options because {}",
                               std::string {e.what()});
        return EXIT_FAILURE;
    }   
    if (getenv("OTEL_SERVICE_NAME") == nullptr)
    {
        constexpr int overwrite{1};
        setenv("OTEL_SERVICE_NAME",
               programOptions.applicationName.c_str(),
               overwrite);
    }

    // Create the logger
    std::shared_ptr<spdlog::logger> logger{nullptr};
    try 
    {   
        logger = UDataPacketBroker::Logger::initialize(programOptions);
    }   
    catch (const std::exception &e) 
    {   
        SPDLOG_LOGGER_CRITICAL(consoleLogger,
                               "Failed to initialize logger because {}",
                               std::string {e.what()});
        return EXIT_FAILURE;
    }   

    // Initialize the metrics
    try
    {
        UDataPacketBroker::Metrics::initialize(programOptions);
    }   
    catch (const std::exception &e) 
    {
        SPDLOG_LOGGER_CRITICAL(logger,
                               "Failed to initialize metrics because {}",
                               std::string {e.what()});
        UDataPacketBroker::Logger::cleanup();
        return EXIT_FAILURE;
    }


    // Initialize the big process
    std::unique_ptr<::Process> process{nullptr};
    try
    {
        //process = std::make_unique<::Process> (programOptions, std::move(logger));
    }
    catch (const std::exception &e)
    {
        SPDLOG_LOGGER_CRITICAL(logger,
                               "Failed to initialize process because {}",
                               e.what());
        UDataPacketBroker::Metrics::cleanup();
        UDataPacketBroker::Logger::cleanup();
        return EXIT_FAILURE;
    }
 
    try
    {

    }
    catch (const std::exception &e)
    {
        SPDLOG_LOGGER_CRITICAL(logger,
                               "Process terminated with critical error {}",
                               e.what());
        UDataPacketBroker::Metrics::cleanup();
        UDataPacketBroker::Logger::cleanup();
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
