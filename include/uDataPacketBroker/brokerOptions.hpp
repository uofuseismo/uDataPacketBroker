#ifndef UDATA_PACKET_BROKER_BROKER_OPTIONS_HPP
#define UDATA_PACKET_BROKER_BROKER_OPTIONS_HPP
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
namespace UDataPacketBroker
{
 class PublishServiceOptions;
 class SubscribeServiceOptions;
}
namespace UDataPacketBroker
{
/// @class BrokerOptions brokerOptions.hpp
/// @brief Sets the options defining the behavior of the broker.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class BrokerOptions
{
public:
    /// @brief Constructor.
    BrokerOptions();
    /// @brief Copy constructor.
    BrokerOptions(const BrokerOptions &options);
    /// @brief Move constructor.
    BrokerOptions(BrokerOptions &&options) noexcept;

    /// @brief Sets the publish service options.  This is where picks are
    ///        submitted by publishers.
    /// @param[in] options  The publish service options.
    void setPublishServiceOptions(const PublishServiceOptions &options);
    /// @result The publish service options.
    /// @throws std::runtime_error if \c hasPublishServiceOptions() is false.
    [[nodiscard]] PublishServiceOptions getPublishServiceOptions() const;
    /// @result True indicates the publish service options were set.
    [[nodiscard]] bool hasPublishServiceOptions() const noexcept;

    /// @brief Sets the subscribe service options.  This is where picks are
    ///        consumed by subscribers.
    /// @param[in] options  The subscribe service options.
    void setSubscribeServiceOptions(const SubscribeServiceOptions &options);
    /// @result The subscribe service options.
    /// @throws std::runtime_error if \c hasSubscribeServiceOptions() is false.
    [[nodiscard]] SubscribeServiceOptions getSubscribeServiceOptions() const;
    /// @result True indicates the subscribe service options were set.
    [[nodiscard]] bool hasSubscribeServiceOptions() const noexcept;

    /// @brief Sets the compaction interval.  The compaction thread in the
    ///        broker wakes up every approximatley every this many
    ///        seconds and removes packets who have exceeded the retention
    ///        interval.
    /// @param[in] interval   The interval at which to run compaction.
    /// @throws std::invalid_argument if this is not positive.
    void setCompactionInterval(const std::chrono::seconds &interval);
    /// @result The compaction interval.
    /// note By default this is 30 seconds.
    [[nodiscard]] std::chrono::seconds getCompactionInterval() const noexcept;

    /// @brief Sets the retention duration of data.  Data received 
    ///        at approximately Now() - getRetentionDuration() are automatically
    ///        purged.
    /// @param[in] duration  The retention duration.
    /// @throw std::invalid_argument if this is not positive.
    void setRetentionDuration(const std::chrono::seconds &duration);
    /// @result The data retention duration.
    /// @note By default this is 30 minutes.
    [[nodiscard]] std::chrono::seconds getRetentionDuration() const noexcept;

    /// @brief Sets the desired maximum data store size in bytes.  
    ///        There will be a thread that attempts to keep the data
    ///        store's memory footprint smaller than this so that hard
    ///        scheduler limits aren't hit.
    /// @param[in] sizeInBytes  The desired size in bytes.
    /// @throws std::invalid_argument if this is not positive.
    void setDesiredMaximumDataStoreSizeInBytes(const size_t sizeInBytes);
    /// @result The desired maximum data store size in bytes.  
    /// @note If this is std::nullopt then it is assumed that you have
    ///       effectively unlimited resources available and truncation
    ///       will not occur.
    [[nodiscard]] std::optional<std::size_t> getDesiredMaximumDataStoreSizeInBytes() const noexcept;

    /// @brief When truncating we purge this many packets at a time.
    /// @param[in] nPackets  The number of packets to purge when trying
    ///                      to get the data store back to an allowable size.
    /// @throws std::invalid_argument if this is not positive.
    void setTruncationChunkSize(int nPackets);
    /// @reuslt The number of packets to purge during a truncation iteration.
    [[nodiscard]] int getTruncationChunkSize() const noexcept;

    /// @brief The base interval at which to truncate the database.
    ///        This attempts to dynamically adapt to the workload since
    ///        the pressure is usually on for short periods of time.
    /// @param[in] baseInterval  The base interval.  If we must truncate
    ///                          the database this will be halved every time. 
    /// @throws std::invalid_argument if this is not positive. 
    void setTruncationBaseInterval(const std::chrono::seconds &baseInterval);
    /// @note By default this is 30 seconds. 
    [[nodiscard]] std::chrono::seconds getTruncationBaseInterval() const noexcept; 

    /// @brief Destructor.
    ~BrokerOptions();
    /// @brief Copy assignment.
    BrokerOptions& operator=(const BrokerOptions &options);
    /// @brief Move assignment.
    BrokerOptions& operator=(BrokerOptions &&options) noexcept;
private:
    class BrokerOptionsImpl;
    std::unique_ptr<BrokerOptionsImpl> pImpl;
};
}
#endif
