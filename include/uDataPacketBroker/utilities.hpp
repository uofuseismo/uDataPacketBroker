#ifndef UDATA_PACKET_BROKER_UTILITIES_HPP
#define UDATA_PACKET_BROKER_UTILITIES_HPP
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
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

//---------------------------------------------------------------------------//
//                              RocksDB key layout                            //
//---------------------------------------------------------------------------//
/// The fixed-width RocksDB key layout for the broker.  Each identifier
/// component is uppercase-normalized and right-padded with 0x00 to its width,
/// and the sequence number is stored big-endian so that a byte-wise comparison
/// (RocksDB's default) orders keys by
/// (network, station, location, channel, sequenceNumber).  See
/// notes/rocksdb-key-design.md.
///
///   packet key : network(8) | station(8) | location(8) | channel(8) | seq(8)
///   stream key : network(8) | station(8) | location(8) | channel(8)
///
/// The stream key is both the `streams` column family key and the prefix used
/// to seek within the `packets` column family.
inline constexpr std::size_t networkKeyLength{8};
inline constexpr std::size_t stationKeyLength{8};
inline constexpr std::size_t locationKeyLength{8};
inline constexpr std::size_t channelKeyLength{8};
/// The stream key length (network + station + location + channel).
inline constexpr std::size_t streamKeyLength{networkKeyLength + stationKeyLength
                                           + locationKeyLength + channelKeyLength};
/// The big-endian sequence number length.
inline constexpr std::size_t sequenceNumberKeyLength{8};
/// The packet key length (stream key + sequence number).
inline constexpr std::size_t packetKeyLength{streamKeyLength
                                           + sequenceNumberKeyLength};

/// @brief Builds the fixed-width stream key for the given identifier.  This is
///        the `streams` column family key and the prefix for seeking the
///        `packets` column family.
/// @result The 32-byte stream key or a reason the identifier could not be
///         encoded (e.g., a component exceeds its width or contains a null).
[[nodiscard]] auto toStreamKey(
    const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier)
    -> std::expected<std::string, std::string>;
/// @brief Builds the fixed-width packet key for the given identifier and
///        broker-assigned sequence number.
/// @result The 40-byte packet key or a reason the identifier could not be
///         encoded.
[[nodiscard]] auto toPacketKey(
    const UDataPacketBrokerAPI::V1::StreamIdentifier &identifier,
    uint64_t sequenceNumber)
    -> std::expected<std::string, std::string>;
/// @brief Decodes a stream key back into an identifier.  The location code is
///        never empty on output - an all-zero slot decodes to "--".
/// @param[in]  key         The 32-byte stream key.
/// @param[out] identifier  The decoded identifier.
[[nodiscard]] auto fromStreamKey(
    std::string_view key,
    UDataPacketBrokerAPI::V1::StreamIdentifier &identifier)
    -> std::expected<void, std::string>;
/// @brief Decodes a packet key back into an identifier and sequence number.
/// @param[in]  key             The 40-byte packet key.
/// @param[out] identifier      The decoded identifier.
/// @param[out] sequenceNumber  The decoded sequence number.
[[nodiscard]] auto fromPacketKey(
    std::string_view key,
    UDataPacketBrokerAPI::V1::StreamIdentifier &identifier,
    uint64_t &sequenceNumber)
    -> std::expected<void, std::string>;

}
#endif
