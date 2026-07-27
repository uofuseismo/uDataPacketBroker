#include <cstdint>
#include <atomic>
#include <algorithm>
#include "uDataPacketBroker/metricsSingleton.hpp"

using namespace UDataPacketBroker;


MetricsSingleton &MetricsSingleton::getInstance()
{
    //std::mutex mutex;
    //const std::scoped_lock lock{mutex};
    static MetricsSingleton instance;
    return instance;
}

void MetricsSingleton::resetCounters()
{

}

void UDataPacketBroker::initializeMetricsSingleton()
{
    MetricsSingleton::getInstance();
}

