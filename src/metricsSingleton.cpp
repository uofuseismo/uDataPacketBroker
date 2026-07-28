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

/// Reset
void MetricsSingleton::resetMetrics() noexcept
{
    mTotalNumberOfPacketsReceived.store(0, std::memory_order_relaxed);
    mNumberOfInvalidPacketsReceived.store(0, std::memory_order_relaxed);
    mNumberOfPacketsAddedToImportQueue.store(0, std::memory_order_relaxed);
    mNumberOfPacketsPoppedFromImportQueue.store(0, std::memory_order_relaxed);
    mPublishServiceUtilization.store(0, std::memory_order_relaxed);
}

/// Publish service utilization
void MetricsSingleton::updatePublishServiceUtilization(double utilization)
{
    mPublishServiceUtilization.store(std::min(std::max(0.0, utilization), 1.0),
                                     std::memory_order_relaxed);
}

double MetricsSingleton::getPublishServiceUtilization() const noexcept
{
    return mPublishServiceUtilization.load(std::memory_order_relaxed);
}

/// Publish service total packets received
void MetricsSingleton::incrementTotalNumberOfPacketsReceived() noexcept
{
    mTotalNumberOfPacketsReceived.fetch_add(1, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getTotalNumberOfPacketsReceived() const noexcept
{
    return mTotalNumberOfPacketsReceived.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementNumberOfInvalidPacketsReceived() noexcept
{
    mNumberOfInvalidPacketsReceived.fetch_add(1, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getNumberOfInvalidPacketsReceived() const noexcept
{
    return mNumberOfInvalidPacketsReceived.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementNumberOfPacketsWrittenToImportQueue() noexcept
{
    mNumberOfPacketsAddedToImportQueue.fetch_add(1, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getNumberOfPacketsWrittenToImportQueue() const noexcept
{
    return mNumberOfPacketsAddedToImportQueue.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementNumberOfPacketsPoppedFromImportQueue(
    const int n) noexcept
{
    mNumberOfPacketsPoppedFromImportQueue.fetch_add(
        std::max(0, n), std::memory_order_relaxed);
}

int64_t MetricsSingleton::getNumberOfPacketsPoppedFromImportQueue() const noexcept
{
    return mNumberOfPacketsPoppedFromImportQueue.load(
        std::memory_order_relaxed);
}


/// Initialize
void UDataPacketBroker::initializeMetricsSingleton()
{
    MetricsSingleton::getInstance();
}

