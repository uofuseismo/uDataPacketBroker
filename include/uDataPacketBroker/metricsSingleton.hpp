#ifndef UDATA_PACKET_BROKER_METRICS_HPP
#define UDATA_PACKET_BROKER_METRICS_HPP
#include <atomic>
#include <cstdint>
namespace UDataPacketBroker
{
/// @brief Easy access for anything needing the application metrics.
class MetricsSingleton
{
public:
    /// @result An instance of the singleton.
    [[maybe_unused]] static MetricsSingleton &getInstance();

    /// @name Publish Service
    /// @{

    /// @brief Updates the publish service utilization.
    void updatePublishServiceUtilization(double utilization);
    /// @result The current publish service utilization.
    [[nodiscard]] double getPublishServiceUtilization() const noexcept;

    /// @brief Increments the total number of packets received.
    ///        This is good (valid) + bad (invalid) + exceptional.
    void incrementTotalNumberOfPacketsReceived() noexcept;
    /// @result The total number of packets received.
    [[nodiscard]] int64_t getTotalNumberOfPacketsReceived() const noexcept;

    /// @brief Number of invalid packets received.
    void incrementNumberOfInvalidPacketsReceived() noexcept;
    /// @result The number of invalid packets received.
    [[nodiscard]] int64_t getNumberOfPacketsAddedToImportQueue() const noexcept;

    /// @brief Increments the number of packets added to the import queue.
    void incrementNumberOfPacketsWrittenToImportQueue() noexcept;
    /// @result The number of invalid packets received.
    [[nodiscard]] int64_t getNumberOfPacketsWrittenToImportQueue() const noexcept;

    /// @brief Increments the number of packets popped from the import queue.
    /// @note This indicates an overflow problem and config should be adjusted
    ///       if it keeps happening.
    void incrementNumberOfPacketsPoppedFromImportQueue(int n) noexcept;
    /// @result The number of packets popped from the import queue.
    [[nodiscard]] int64_t getNumberOfPacketsPoppedFromImportQueue() const noexcept;


    /// @}

    /// @name Middleware
    /// @{

    /// @brief The number of packets submitted to be published.
    void incrementNumberOfSentToPublisher();
    /// @result The number of packets submitted to be published.
    [[nodiscard]] int64_t getNumberOfPacketsSentToPublisher() noexcept;

    /// @}
 
    /// @brief Updates

    /// @brief Resets the counters an dutilization.  This is useful for unit tests.
    void resetMetrics() noexcept;
private:
    MetricsSingleton() = default;
    ~MetricsSingleton() = default;
    std::atomic<int64_t> mTotalNumberOfPacketsReceived{0};
    std::atomic<int64_t> mNumberOfInvalidPacketsReceived{0};
    std::atomic<int64_t> mNumberOfPacketsAddedToImportQueue{0};
    std::atomic<int64_t> mNumberOfPacketsPoppedFromImportQueue{0};
    std::atomic<double> mPublishServiceUtilization{0};
};
/// @brief Initializes the metrics singleton once and for all.  This is to be
///        used at application start up.
void initializeMetricsSingleton();
}
#endif
