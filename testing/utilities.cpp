#include <cstdint>
#include <string>
#include <uDataPacketBrokerAPI/v1/stream_identifier.pb.h>
#include "uDataPacketBroker/utilities.hpp"
#include <catch2/catch_test_macros.hpp>

namespace
{

namespace Utilities = UDataPacketBroker::Utilities;
using UDataPacketBrokerAPI::V1::StreamIdentifier;

[[nodiscard]]
StreamIdentifier makeIdentifier(const std::string &network,
                                const std::string &station,
                                const std::string &channel,
                                const std::string &locationCode)
{
    StreamIdentifier identifier;
    identifier.set_network(network);
    identifier.set_station(station);
    identifier.set_channel(channel);
    identifier.set_location_code(locationCode);
    return identifier;
}

}

TEST_CASE("RocksDB key lengths", "[utilities][keys]")
{
    REQUIRE(Utilities::streamKeyLength == 32);
    REQUIRE(Utilities::packetKeyLength == 40);
}

TEST_CASE("Stream key is fixed width and zero padded", "[utilities][keys]")
{
    auto identifier = makeIdentifier("UU", "SRU", "HHZ", "01");
    auto key = Utilities::toStreamKey(identifier);
    REQUIRE(key.has_value());
    REQUIRE(key->size() == Utilities::streamKeyLength);
    // "UU" then six 0x00, "SRU" then five 0x00, ...
    REQUIRE(key->substr(0, 2) == "UU");
    REQUIRE((*key)[2] == '\0');
    REQUIRE(key->substr(8, 3) == "SRU");
    REQUIRE(key->substr(16, 2) == "01");
    REQUIRE(key->substr(24, 3) == "HHZ");
}

TEST_CASE("Packet key is the stream key plus a big-endian sequence",
          "[utilities][keys]")
{
    auto identifier = makeIdentifier("UU", "SRU", "HHZ", "01");
    auto streamKey = Utilities::toStreamKey(identifier);
    auto packetKey = Utilities::toPacketKey(identifier, 0x0102030405060708ULL);
    REQUIRE(streamKey.has_value());
    REQUIRE(packetKey.has_value());
    REQUIRE(packetKey->size() == Utilities::packetKeyLength);
    // The packet key must begin with the stream key so a stream key is a valid
    // seek prefix into the packets column family.
    REQUIRE(packetKey->starts_with(*streamKey));
    // Big-endian: most significant byte first.
    REQUIRE(static_cast<unsigned char> ((*packetKey)[32]) == 0x01);
    REQUIRE(static_cast<unsigned char> ((*packetKey)[39]) == 0x08);
}

TEST_CASE("Packet keys order by channel then sequence", "[utilities][keys]")
{
    auto identifier = makeIdentifier("UU", "SRU", "HHZ", "01");
    auto seq1 = Utilities::toPacketKey(identifier, 1);
    auto seq2 = Utilities::toPacketKey(identifier, 2);
    auto seqBig = Utilities::toPacketKey(identifier, 0xFFFFFFFFFFULL);
    REQUIRE(*seq1 < *seq2);
    REQUIRE(*seq2 < *seqBig);

    // Channel ordering dominates the sequence number.
    auto ehz = Utilities::toPacketKey(makeIdentifier("UU", "SRU", "EHZ", "01"),
                                      0xFFFFFFFFFFULL);
    auto hhzLow = Utilities::toPacketKey(makeIdentifier("UU", "SRU", "HHZ", "01"),
                                         0);
    REQUIRE(*ehz < *hhzLow);
}

TEST_CASE("Zero padding preserves station prefix ordering", "[utilities][keys]")
{
    // The classic hazard: "SR" must sort before "SRU" because 0x00 < 'U'.
    auto shortSta = Utilities::toStreamKey(makeIdentifier("UU", "SR", "HHZ", "01"));
    auto longSta = Utilities::toStreamKey(makeIdentifier("UU", "SRU", "HHZ", "01"));
    REQUIRE(shortSta.has_value());
    REQUIRE(longSta.has_value());
    REQUIRE(*shortSta < *longSta);
    // And they occupy distinct slots (no prefix collision).
    REQUIRE(*shortSta != *longSta);
}

TEST_CASE("Empty, unset, and '--' location codes fold to one key",
          "[utilities][keys]")
{
    auto dashes = Utilities::toStreamKey(makeIdentifier("UU", "SRU", "HHZ", "--"));
    auto empty = Utilities::toStreamKey(makeIdentifier("UU", "SRU", "HHZ", ""));
    StreamIdentifier unset;
    unset.set_network("UU");
    unset.set_station("SRU");
    unset.set_channel("HHZ");
    auto unsetKey = Utilities::toStreamKey(unset);
    REQUIRE(dashes.has_value());
    REQUIRE(empty.has_value());
    REQUIRE(unsetKey.has_value());
    REQUIRE(*dashes == *empty);
    REQUIRE(*dashes == *unsetKey);
}

TEST_CASE("Codes are uppercase-normalized", "[utilities][keys]")
{
    // A lowercase publisher must merge into the same stream, and decode back
    // uppercase.
    auto upper = Utilities::toStreamKey(makeIdentifier("UU", "SRU", "HHZ", "01"));
    auto lower = Utilities::toStreamKey(makeIdentifier("uu", "sru", "hhz", "01"));
    REQUIRE(upper.has_value());
    REQUIRE(lower.has_value());
    REQUIRE(*upper == *lower);

    StreamIdentifier decoded;
    auto result = Utilities::fromStreamKey(*lower, decoded);
    REQUIRE(result.has_value());
    REQUIRE(decoded.network() == "UU");
    REQUIRE(decoded.station() == "SRU");
    REQUIRE(decoded.channel() == "HHZ");
}

TEST_CASE("Round trip encode then decode", "[utilities][keys]")
{
    auto identifier = makeIdentifier("UU", "SRU", "HHZ", "01");
    constexpr uint64_t sequenceNumber{8412};
    auto key = Utilities::toPacketKey(identifier, sequenceNumber);
    REQUIRE(key.has_value());

    StreamIdentifier decoded;
    uint64_t decodedSequenceNumber{0};
    auto result = Utilities::fromPacketKey(*key, decoded, decodedSequenceNumber);
    REQUIRE(result.has_value());
    REQUIRE(decoded.network() == "UU");
    REQUIRE(decoded.station() == "SRU");
    REQUIRE(decoded.channel() == "HHZ");
    REQUIRE(decoded.location_code() == "01");
    REQUIRE(decodedSequenceNumber == sequenceNumber);
}

TEST_CASE("Decoded location code is never empty", "[utilities][keys]")
{
    auto key = Utilities::toStreamKey(makeIdentifier("UU", "SRU", "HHZ", ""));
    REQUIRE(key.has_value());
    StreamIdentifier decoded;
    auto result = Utilities::fromStreamKey(*key, decoded);
    REQUIRE(result.has_value());
    REQUIRE(decoded.location_code() == "--");
}

TEST_CASE("Over-length components are rejected, not truncated",
          "[utilities][keys]")
{
    // 9 bytes of channel > channelKeyLength (8).
    auto tooLong = Utilities::toPacketKey(
        makeIdentifier("UU", "SRU", "HHZABCDEF", "01"), 1);
    REQUIRE_FALSE(tooLong.has_value());

    auto emptyNetwork = Utilities::toStreamKey(
        makeIdentifier("", "SRU", "HHZ", "01"));
    REQUIRE_FALSE(emptyNetwork.has_value());
}

TEST_CASE("Embedded null bytes are rejected", "[utilities][keys]")
{
    auto withNull = Utilities::toStreamKey(
        makeIdentifier("UU", std::string("SR\0U", 4), "HHZ", "01"));
    REQUIRE_FALSE(withNull.has_value());
}

TEST_CASE("Decoding a wrong-size key fails", "[utilities][keys]")
{
    StreamIdentifier decoded;
    auto result = Utilities::fromStreamKey(std::string(31, '\0'), decoded);
    REQUIRE_FALSE(result.has_value());
}