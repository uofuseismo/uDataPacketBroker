#include <atomic>
#include <algorithm>
#include <cstdint>
#include "uDataPacketBroker/metricsSingleton.hpp"

using namespace UDataPacketBroker;


MetricsSingleton &MetricsSingleton::getInstance()
{
    //std::mutex mutex;
    //const std::scoped_lock lock{mutex};
    static MetricsSingleton instance;
    return instance;
}

void MetricsSingleton::resetMetrics() noexcept
{
    mPublishServiceUtilization.store(0, std::memory_order_relaxed);
}

void MetricsSingleton::updatePublishServiceUtilization(double utilization)
{
    mPublishServiceUtilization.store(std::min(std::max(0.0, utilization), 1.0),
                                     std::memory_order_relaxed);
}

double MetricsSingleton::getPublishServiceUtilization() const noexcept
{
    return mPublishServiceUtilization.load(std::memory_order_relaxed);
}

void UDataPacketBroker::initializeMetricsSingleton()
{
    MetricsSingleton::getInstance();
}

