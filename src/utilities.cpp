#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <uDataPacketBrokerAPI/v1/packet.pb.h>
#include <uDataPacketBrokerAPI/v1/stream_identifier.pb.h>
#include "uDataPacketBroker/utilities.hpp"

namespace
{

/// Appends a code, uppercased and right-padded with 0x00, to the key.  Codes
/// are uppercase-normalized so a misbehaving lowercase publisher merges into
/// the correct stream rather than spawning a phantom.  A code that is too long
/// or that contains an embedded null would corrupt the fixed-width layout, so
/// it is rejected rather than truncated (per notes/rocksdb-key-design.md).
[[nodiscard]]
std::expected<void, std::string> appendPaddedComponent(
    std::string &key,
    std::string_view value,
    std::string_view fieldName,
    std::size_t width)
{
    if (value.empty())
    {
        return std::unexpected(std::string {fieldName}.append(" is empty"));
    }
    if (value.size() > width)
    {
        return std::unexpected(
            std::string {fieldName}.append(" '").append(value)
                .append("' exceeds ").append(std::to_string(width))
                .append(" bytes"));
    }
    if (value.find('\0') != std::string_view::npos)
    {
        return std::unexpected(
            std::string {fieldName}.append(" contains an embedded null byte"));
    }
    for (const char character : value)
    {
        key.push_back(static_cast<char> (
            std::toupper(static_cast<unsigned char> (character))));
    }
    key.append(width - value.size(), '\0');
    return {};
}

/// Appends a 64-bit unsigned integer big-endian so that byte-wise comparison
/// matches numeric ordering.
void appendBigEndian64(std::string &key, uint64_t value)
{
    for (int shift = 56; shift >= 0; shift = shift - 8)
    {
        key.push_back(static_cast<char> ((value >> shift) & 0xFFU));
    }
}

/// Reads a big-endian 64-bit unsigned integer from the key at the given offset.
[[nodiscard]]
uint64_t readBigEndian64(std::string_view key, std::size_t offset)
{
    uint64_t value{0};
    for (std::size_t i = 0; i < 8; ++i)
    {
        value = (value << 8)
              | static_cast<unsigned char> (key[offset + i]);
    }
    return value;
}

/// Recovers a code from a fixed-width slot by stripping the 0x00 padding.
[[nodiscard]]
std::string readPaddedComponent(std::string_view key,
                                std::size_t offset,
                                std::size_t width)
{
    auto slot = key.substr(offset, width);
    auto end = slot.find('\0');
    if (end != std::string_view::npos)
    {
        slot = slot.substr(0, end);
    }
    return std::string {slot};
}

/// The location code that will be written into the key.  An unset or empty
/// location folds to "--" so that "", unset, and "--" all map to one stream.
[[nodiscard]]
std::string canonicalLocationCode(
    const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier)
{
    if (identifier.has_location_code())
    {
        const auto &locationCode = identifier.location_code();
        if (!locationCode.empty())
        {
            return locationCode;
        }
    }
    return "--";
}

}
/// Gets the current time in microseconds 
template<>
std::chrono::microseconds UDataPacketBroker::Utilities::getNow()
{
    auto now 
       = std::chrono::duration_cast<std::chrono::microseconds>
         ((std::chrono::high_resolution_clock::now()).time_since_epoch());
    return now;
}   

/// Gets the current time in nanoseconds
template<>
std::chrono::nanoseconds UDataPacketBroker::Utilities::getNow()
{
    auto now 
       = std::chrono::duration_cast<std::chrono::nanoseconds>
         ((std::chrono::high_resolution_clock::now()).time_since_epoch());
    return now;
}   

/// 
std::string UDataPacketBroker::Utilities::toString(
    const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier)
{
    std::string result;
    result = identifier.network(); 
    result.append(".");
    result.append(identifier.station());
    result.append(".");
    result.append(identifier.channel());
    result.append(".");
    std::string locationCode;
    if (identifier.has_location_code())
    {
        locationCode = identifier.location_code();
    }
    if (!locationCode.empty())
    {
        result.append(locationCode);
    }
    else
    {
        result.append("--");
    }
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

/// Validate the input packet - true  means it's good to go.
/// This is pretty generous since the broker is more of a passthrough than
/// anything.  Other applications can deal with empty packets, packets with
/// bad timing, etc.
auto UDataPacketBroker::Utilities::isValid(
    const UDataPacketBrokerAPI::V1::Packet &packet) -> std::expected<bool, std::string>
{
    if (!packet.has_stream_identifier())   
    {
        return std::unexpected("Packet does not have an identifier");
    }
    const auto &identifier = packet.stream_identifier();
    if (!identifier.has_network())
    {
        return std::unexpected("Network not set in packet identifier");
    }
    if (!identifier.has_station())
    {
        return std::unexpected("Station not set in packet identifier");
    }
    if (!identifier.has_channel())
    {
        return std::unexpected("Channel not set in packet identifier");
    }
    if (!packet.has_start_time())
    {
        auto name = toString(identifier);
        return std::unexpected(name.append(" does not have a start time."));
    }
    if (!packet.has_sampling_rate())
    {
        auto name = toString(identifier);
        return std::unexpected(name.append(" does not have a sampling rate."));
    }
    if (packet.sampling_rate() <= 0)
    {   
        auto name = toString(identifier);
        return std::unexpected(name.append(" sampling rate not positive."));
    }
    if (!packet.has_data_type())
    {   
        auto name = toString(identifier);
        return std::unexpected(name.append(" does not have a data type."));
    }
    if (!packet.has_number_of_samples())
    {   
        auto name = toString(identifier);
        return std::unexpected(name.append(" does not have any samples."));
    }
    // No sequence number is okay - basically the client screwing themselves
    return true;
}

/// Builds the 32-byte stream key (network|station|location|channel).
auto UDataPacketBroker::Utilities::toStreamKey(
    const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier)
    -> std::expected<std::string, std::string>
{
    std::string key;
    key.reserve(streamKeyLength);
    if (auto result = ::appendPaddedComponent(
            key, identifier.network(), "Network", networkKeyLength); !result)
    {
        return std::unexpected(result.error());
    }
    if (auto result = ::appendPaddedComponent(
            key, identifier.station(), "Station", stationKeyLength); !result)
    {
        return std::unexpected(result.error());
    }
    if (auto result = ::appendPaddedComponent(
            key, ::canonicalLocationCode(identifier), "Location",
            locationKeyLength); !result)
    {
        return std::unexpected(result.error());
    }
    if (auto result = ::appendPaddedComponent(
            key, identifier.channel(), "Channel", channelKeyLength); !result)
    {
        return std::unexpected(result.error());
    }
    return key;
}

/// Builds the 40-byte packet key (stream key | big-endian sequence number).
auto UDataPacketBroker::Utilities::toPacketKey(
    const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier,
    uint64_t sequenceNumber)
    -> std::expected<std::string, std::string>
{
    auto key = toStreamKey(identifier);
    if (!key)
    {
        return std::unexpected(key.error());
    }
    key->reserve(packetKeyLength);
    ::appendBigEndian64(*key, sequenceNumber);
    return key;
}

/// Decodes a 32-byte stream key back into an identifier.
auto UDataPacketBroker::Utilities::fromStreamKey(
    std::string_view key,
    UDataPacketBrokerAPI::V1::StreamIdentifier &identifier)
    -> std::expected<void, std::string>
{
    if (key.size() != streamKeyLength)
    {
        return std::unexpected(
            "Stream key has " + std::to_string(key.size())
          + " bytes; expected " + std::to_string(streamKeyLength));
    }
    identifier.set_network(
        ::readPaddedComponent(key, 0, networkKeyLength));
    identifier.set_station(
        ::readPaddedComponent(key, networkKeyLength, stationKeyLength));
    auto locationCode = ::readPaddedComponent(
        key, networkKeyLength + stationKeyLength, locationKeyLength);
    identifier.set_location_code(locationCode.empty() ? "--" : locationCode);
    identifier.set_channel(
        ::readPaddedComponent(
            key, networkKeyLength + stationKeyLength + locationKeyLength,
            channelKeyLength));
    return {};
}

/// Decodes a 40-byte packet key back into an identifier and sequence number.
auto UDataPacketBroker::Utilities::fromPacketKey(
    std::string_view key,
    UDataPacketBrokerAPI::V1::StreamIdentifier &identifier,
    uint64_t &sequenceNumber)
    -> std::expected<void, std::string>
{
    if (key.size() != packetKeyLength)
    {
        return std::unexpected(
            "Packet key has " + std::to_string(key.size())
          + " bytes; expected " + std::to_string(packetKeyLength));
    }
    if (auto result = fromStreamKey(key.substr(0, streamKeyLength), identifier);
        !result)
    {
        return std::unexpected(result.error());
    }
    sequenceNumber = ::readBigEndian64(key, streamKeyLength);
    return {};
}
