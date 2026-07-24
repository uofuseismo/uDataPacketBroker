#ifndef UDATA_PACKET_BROKER_UTILITIES_HPP
#define UDATA_PACKET_BROKER_UTILITIES_HPP
#include <chrono>
#include <expected>
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

/// @brief Converts a stream identifier to a string representation like 
///        UU.ELU.EHZ.01.
[[nodiscard]] std::string toString(const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier);
/// @brief Validates a received data packet.
/// @result True indicates the packet is okay otherwise provides
///         a reason for why the packet is invalid.
[[nodiscard]] auto isValid(const UDataPacketBrokerAPI::V1::Packet &packet) -> std::expected<bool, std::string>;

}
#endif
