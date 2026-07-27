#include <cassert>
#include <cstdlib>
#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <uDataPacketBrokerAPI/v1/packet.pb.h>
#include "uDataPacketBroker/broker.hpp"
#include "uDataPacketBroker/metricsSingleton.hpp"
#include "programOptions.hpp"

class Process
{
//public:
    Process(ProgramOptions &&options,
            std::shared_ptr<spdlog::logger> logger) :
        mOptions(std::move(options)),
        mLogger(std::move(logger))
    {
#ifndef NDEBUG
        assert(mLogger != nullptr);
#endif

    }
/*
    /// @brief Used by the subscriber to set the latest packet.
    void addPacketCallback(UDataPacketBrokerAPI::V1::Packet &&packet)
    {
        /// Validate the packet
        auto isValid = UDataPacketBroker::Utilities::isValid(packet);
        if (isValid.has_value())
        {
// TODO i think this is going to have multiple writers so i should do a mutex and dump tbb
            int nPopped{0};
            if (mImportQueue.size() >= mMaximumImportQueueSize)
            {
                nPopped = nPopped + 1;
                while (mImportQueue.size() >= mMaximumImportQueueSize)
                {
                    UDataPacketBrokerAPI::V1::Packet work;
                    // Queues are FIFO Oldest element loses
                    if (!mImportQueue.try_pop(work))
                    {
                        SPDLOG_LOGGER_WARN(mLogger, "Failed to pop end of queue");
                        break;
                    }
                }
            }
        }
        else
        {
            // Update metrics and return
 
        }
    }
*/
//private:
    ::ProgramOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::chrono::seconds mLastReport
    {
        std::chrono::duration_cast<std::chrono::seconds>
        ((std::chrono::high_resolution_clock::now()).time_since_epoch())
    };
    std::map<std::string, std::future<void>> mFuturesMap;
    std::unique_ptr<UDataPacketBroker::Broker> mBroker{nullptr};
};

int main(int argc, char *argv[])
{
    UDataPacketBroker::initializeMetricsSingleton();
    return EXIT_SUCCESS;
}
