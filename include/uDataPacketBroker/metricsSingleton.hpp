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
 
    /// @brief Resets the counters and clears maps.  This is useful for unit tests.
    void resetCounters();
private:
    MetricsSingleton() = default;
    ~MetricsSingleton() = default;
};
/// @brief Initializes the metrics singleton once and for all.  This is to be
///        used at application start up.
void initializeMetricsSingleton();
}
#endif
