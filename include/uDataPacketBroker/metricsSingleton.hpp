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

    /// @brief Updates the publish service utilization.
    void updatePublishServiceUtilization(double utilization);
    /// @result The current publish service utilization.
    [[nodiscard]] double getPublishServiceUtilization() const noexcept;
 
    /// @brief Resets the counters an dutilization.  This is useful for unit tests.
    void resetMetrics() noexcept;
private:
    MetricsSingleton() = default;
    ~MetricsSingleton() = default;
    std::atomic<double> mPublishServiceUtilization{0};
};
/// @brief Initializes the metrics singleton once and for all.  This is to be
///        used at application start up.
void initializeMetricsSingleton();
}
#endif
