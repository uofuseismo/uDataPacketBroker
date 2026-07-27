#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>
#include <string>
#include <thread>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include <uDataPacketBrokerAPI/v1/packet.pb.h>
#include "uDataPacketBroker/broker.hpp"
#include "uDataPacketBroker/brokerOptions.hpp"
#include "uDataPacketBroker/dataPacketStore.hpp"
#include "uDataPacketBroker/metricsSingleton.hpp"
#include "uDataPacketBroker/subscribeServiceOptions.hpp"
#include "uDataPacketBroker/publishService.hpp"
#include "uDataPacketBroker/publishServiceOptions.hpp"
#include "uDataPacketBroker/utilities.hpp"

using namespace UDataPacketBroker;

class BrokerImpl
{
public:
    BrokerImpl(const BrokerOptions &options,
               std::shared_ptr<IDataPacketStore> dataPacketStore,
               std::shared_ptr<spdlog::logger> logger) :
        mOptions(options),
        mDataPacketStore(std::move(dataPacketStore)),
        mLogger(std::move(logger))
    {
        if (!mOptions.hasPublishServiceOptions())
        {
            throw std::invalid_argument("Publish service options not set");
        }
        if (!mOptions.hasSubscribeServiceOptions())
        {
            throw std::invalid_argument("Subscribe service options not set");
        }
        if (mDataPacketStore == nullptr)
        {
            throw std::invalid_argument("Data packet store is null");
        } 
        // Logger
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            constexpr const char *loggerName{"BrokerConsole"};
            mLogger = spdlog::get(loggerName);
            if (mLogger == nullptr)
            {
                mLogger = spdlog::stdout_color_mt(loggerName);
            }
            // NOLINTEND(misc-include-cleaner)
        }
        // Get the last written sequence number.  Note, the import thread,
        // which must start after this, will then increment it.
        mGlobalSequenceNumber = mDataPacketStore->getGlobalSequenceNumber();
        // Probably should issue some type of warning if we're within a 
        // million of numerical overflow or just say - you gotta reset this
        // file and crash out.

        // Create the publish service
        mPublishService 
            = std::make_unique<PublishService> 
              (
                  mOptions.getPublishServiceOptions(),
                  mAddPacketCallbackFunction,
                  mLogger
              );
 
    }

    ~BrokerImpl()
    {
        stop();
    }

    /// Starts the threads. 
    void start()
    {
        mKeepRunning.store(true);
        // Start the packet propagator
 
        // Start publishing data

        // Start receiving data 
         
    }

    /// @brief Landing spot for the publish service to send received packets.
    /// @note We clobber the input sequence number here in a mutex so that
    ///       it is ready for all downstream customers.
    void addPacketCallback(UDataPacketBrokerAPI::V1::Packet &&packet)
    {
        mMetrics.incrementTotalNumberOfPacketsReceived();
        /// Validate the packet
        auto isValid = UDataPacketBroker::Utilities::isValid(packet);
        if (!isValid.has_value())
        {
            mMetrics.incrementNumberOfInvalidPacketsReceived();
            throw std::invalid_argument("Input packet invalid: "
                                      + isValid.error());
        }
        // Okay, get it in the queue.
        int nPopped{0};
        {
        std::lock_guard<std::mutex> lock(mImportMutex);
        // Space available?
        if (mImportQueue.size() > mMaximumImportQueueSize)
        {
            while (mImportQueue.size() >= mMaximumImportQueueSize)
            {
                mImportQueue.pop();
                nPopped = nPopped + 1;
            }
        }
        mImportQueue.push(std::move(packet));
        }
        // Problem detected - update overflow metrics and warn
        if (nPopped > 0)
        {
            mMetrics.incrementNumberOfPacketsPoppedFromImportQueue(nPopped);
            SPDLOG_LOGGER_WARN(mLogger,
                               "Import packet overflow - purged {} packets",
                               nPopped);
        }
        // Update this has been added
        mMetrics.incrementNumberOfInvalidPacketsReceived();
    }

    /// @brief This thread reads input packets and gets them to outbound
    ///        writer threads (while updating sequence numbers).
    ///        This is a fan-out approach:
    ///                 Input Queue
    ///                     |
    ///              -----------------
    ///              |               |
    ///         Publish Queue     Database Queue
    void propagatePacket()
    {
        while (mKeepRunning.load())
        {
            // Check for new packet
            bool gotPacket{false};
            UDataPacketBrokerAPI::V1::Packet packet;
            {
            std::lock_guard<std::mutex> lock(mImportMutex);
            if (!mImportQueue.empty())
            {
                packet = std::move(mImportQueue.front());
                mImportQueue.pop();
                gotPacket = true;
            }
            }
            if (gotPacket)
            {
                // New packet received - update sequence number and ship it off
                mGlobalSequenceNumber = mGlobalSequenceNumber + 1;
                packet.set_sequence_number(mGlobalSequenceNumber);
                {
                std::lock_guard<std::mutex> lock(mPublishMutex);
                mPublishQueue.push(packet);
                // Can handle max sizing here too

                } 
                {
                std::lock_guard<std::mutex> lock(mDataPacketStoreMutex);
                // N.B. This is a slight optimization since I'm done with
                // the packet.
                mDataPacketStoreQueue.push(std::move(packet));
                // Can handle max sizing here too

                }
            }
            else
            {
                constexpr std::chrono::milliseconds timeOut{10};
                std::this_thread::sleep_for(timeOut);
            }
        }
        SPDLOG_LOGGER_DEBUG(mLogger, "Exiting propagation loop");
    }

    /// @brief Writes the packets to the subscription manager to make available
    ///        via gRPC.
    void publishPackets()
    {
        while (mKeepRunning.load())
        {
            // Check for new packet
            bool gotPacket{false};
            UDataPacketBrokerAPI::V1::Packet packet;
            {
            std::lock_guard<std::mutex> lock(mPublishMutex);
            if (!mPublishQueue.empty())
            { 
                packet = std::move(mPublishQueue.front());
                mPublishQueue.pop();
                gotPacket = true;
            }
            }
            if (gotPacket)
            {
                // Enqueue the packet in the writer service
                try
                {

                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_WARN(mLogger,
                                       "Failed to enqueue packet because {}",
                                       e.what());
                    // TODO update metrics
                }
            }
            else
            {
                constexpr std::chrono::milliseconds timeOut{10};
                std::this_thread::sleep_for(timeOut);
            }
        }
        SPDLOG_LOGGER_DEBUG(mLogger, "Exiting publisher loop");
    }

    /// @brief This thread manages database packets.
    /// @note Because there's queues between the initial thread receiving
    ///       the packet, it propagating to this function, this function
    ///       batch writing the data there's a gap where I can lose data.
    void writePacketsToDatabase()
    {
        while (mKeepRunning.load())
        {
            bool gotPacket{false};
            // Have the option to be kinda smart here.  If there's
            // a few packets then I can write them all at once.
            {
            std::lock_guard<std::mutex> lock(mDataPacketStoreMutex);
         
            } 
            // Try again on failed writes. 

            // No data?  Okay, take a nap.
            constexpr std::chrono::milliseconds timeOut{10};
            std::this_thread::sleep_for(timeOut);
        } 
        SPDLOG_LOGGER_DEBUG(mLogger, "Exiting database writer loop");
    }

    /// @brief Stops the broker.
    void stop()
    {
        // Stop the publisher service
        if (mPublishService)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Shutting down publisher");
            mPublishService->stop();
            std::this_thread::sleep_for(std::chrono::milliseconds {10});
        }
        // Stop propagating data.
        SPDLOG_LOGGER_DEBUG(mLogger, "Stopping broker processes");
        mKeepRunning.store(false, std::memory_order_seq_cst);

        // Hopefully by now everything has been sent - end subscriber service.

    }
//private:
    BrokerOptions mOptions;
    std::shared_ptr<IDataPacketStore> mDataPacketStore{nullptr};
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::mutex mImportMutex;
    std::mutex mDataPacketStoreMutex;
    std::mutex mPublishMutex;
    std::unique_ptr<PublishService> mPublishService{nullptr};
    //std::shared_ptr<SubscriptionManager> mSubscriptionManager{nullptr};
    std::function<void(UDataPacketBrokerAPI::V1::Packet &&)>
         mAddPacketCallbackFunction
    {
         std::bind(&BrokerImpl::addPacketCallback, this,
                   std::placeholders::_1)
    };
    UDataPacketBroker::MetricsSingleton &mMetrics
    {   
        UDataPacketBroker::MetricsSingleton::getInstance()
    };  
    std::queue<UDataPacketBrokerAPI::V1::Packet> mImportQueue;
    std::queue<UDataPacketBrokerAPI::V1::Packet> mDataPacketStoreQueue;
    std::queue<UDataPacketBrokerAPI::V1::Packet> mPublishQueue;
    std::map<std::string, std::future<void>> mFuturesMap;
    uint64_t mGlobalSequenceNumber{0};
    size_t mMaximumImportQueueSize{8192};
    std::atomic<bool> mKeepRunning{true};
};
