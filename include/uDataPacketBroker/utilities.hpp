#ifndef UDATA_PACKET_BROKER_UTILITIES_HPP
#define UDATA_PACKET_BROKER_UTILITIES_HPP
#include <chrono>
#include <string>
namespace UDataPacketBrokerAPI::V1
{
 class StreamIdentifier;
 class Packet;
}

namespace UDataPacketBroker::Utilities
{

/// @result The current time in microseconds or nanoseconds.
template<typename T> T getNow();

}
#endif
