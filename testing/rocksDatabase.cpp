#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <uDataPacketBrokerAPI/v1/packet.pb.h>
#include <uDataPacketBrokerAPI/v1/stream_identifier.pb.h>
#include <uDataPacketBrokerAPI/v1/data_type.pb.h>
#include "uDataPacketBroker/rocksDatabase.hpp"
#include "uDataPacketBroker/rocksDatabaseOptions.hpp"
#include "uDataPacketBroker/dataPacketStore.hpp"
#include <catch2/catch_test_macros.hpp>

namespace
{

using namespace UDataPacketBroker;
using UDataPacketBrokerAPI::V1::Packet;
using UDataPacketBrokerAPI::V1::StreamIdentifier;

[[nodiscard]]
StreamIdentifier makeIdentifier(const std::string &channel)
{
    StreamIdentifier identifier;
    identifier.set_network("UU");
    identifier.set_station("SRU");
    identifier.set_channel(channel);
    identifier.set_location_code("01");
    return identifier;
}

[[nodiscard]]
Packet makePacket(const std::string &channel,
                  uint64_t sequenceNumber,
                  char sampleByte)
{
    Packet packet;
    *packet.mutable_stream_identifier() = makeIdentifier(channel);
    packet.set_sampling_rate(100.0);
    packet.set_data_type(UDataPacketBrokerAPI::V1::DATA_TYPE_INTEGER_32);
    packet.set_number_of_samples(4);
    packet.set_data(std::string(16, sampleByte));  // 4 int32 samples
    packet.set_sequence_number(sequenceNumber);
    return packet;
}

[[nodiscard]]
std::filesystem::path uniqueDatabasePath()
{
    const auto unique = std::to_string(std::random_device{}());
    return std::filesystem::temp_directory_path()
         / ("udpb_store_test_" + unique);
}

}

TEST_CASE("RocksDatabase round trip", "[RocksDatabase]")
{
    const auto databasePath = ::uniqueDatabasePath();
    RocksDatabaseOptions options;
    options.setDatabaseDirectory(databasePath);
    options.enableOverWriteIfExists();

    {
        RocksDatabase store(options, nullptr);
        REQUIRE(store.isInitialized());
        REQUIRE(store.getGlobalSequenceNumber() == 0);

        // Two channels interleaved on one global broker counter.  HHZ gets
        // 1, 3, 5; EHZ gets 2, 4.
        std::vector<std::pair<std::chrono::nanoseconds, Packet>> batch;
        batch.emplace_back(std::chrono::nanoseconds {1000},
                           ::makePacket("HHZ", 1, 0x11));
        batch.emplace_back(std::chrono::nanoseconds {2000},
                           ::makePacket("EHZ", 2, 0x22));
        batch.emplace_back(std::chrono::nanoseconds {3000},
                           ::makePacket("HHZ", 3, 0x33));
        batch.emplace_back(std::chrono::nanoseconds {4000},
                           ::makePacket("EHZ", 4, 0x44));
        batch.emplace_back(std::chrono::nanoseconds {5000},
                           ::makePacket("HHZ", 5, 0x55));

        const auto notWritten = store.write(batch);
        REQUIRE(notWritten.empty());
        // Explicitly persist the memtable (what stop() does on graceful
        // shutdown, since the write-ahead-log is disabled).
        REQUIRE_NOTHROW(store.flush());

        SECTION("Global sequence number is the max written")
        {
            REQUIRE(store.getGlobalSequenceNumber() == 5);
        }

        SECTION("Query from a cursor returns the ordered suffix")
        {
            const auto responses
                = store.query({{::makeIdentifier("HHZ"), 3}});
            REQUIRE(responses.size() == 2);
            REQUIRE(responses[0].sequenceNumber == 3);
            REQUIRE(responses[1].sequenceNumber == 5);
            // Receipt time round-trips.
            REQUIRE(responses[0].receivedAt == std::chrono::nanoseconds {3000});
            // Payload round-trips and carries the right channel/seq.
            Packet decoded;
            REQUIRE(decoded.ParseFromString(responses[0].packet));
            REQUIRE(decoded.sequence_number() == 3);
            REQUIRE(decoded.stream_identifier().channel() == "HHZ");
            REQUIRE(decoded.data() == std::string(16, 0x33));
        }

        SECTION("Query does not bleed across channels")
        {
            const auto responses
                = store.query({{::makeIdentifier("EHZ"), 0}});
            REQUIRE(responses.size() == 2);
            REQUIRE(responses[0].sequenceNumber == 2);
            REQUIRE(responses[1].sequenceNumber == 4);
        }

        SECTION("Query past the last sequence is empty")
        {
            REQUIRE(store.query({{::makeIdentifier("HHZ"), 6}}).empty());
        }

        SECTION("Available streams lists both channels with last-received")
        {
            const auto streams = store.queryAvailableStreams();
            REQUIRE(streams.size() == 2);
            bool sawHHZ{false};
            bool sawEHZ{false};
            for (const auto &[lastReceived, identifier] : streams)
            {
                if (identifier.channel() == "HHZ")
                {
                    sawHHZ = true;
                    REQUIRE(lastReceived == std::chrono::nanoseconds {5000});
                }
                else if (identifier.channel() == "EHZ")
                {
                    sawEHZ = true;
                    REQUIRE(lastReceived == std::chrono::nanoseconds {4000});
                }
                REQUIRE(identifier.network() == "UU");
                REQUIRE(identifier.station() == "SRU");
            }
            REQUIRE(sawHHZ);
            REQUIRE(sawEHZ);
        }

        SECTION("Truncate drops the oldest packets across channels")
        {
            REQUIRE(store.getSizeInBytes() > 0);

            // Drop the oldest 2 global seqs (HHZ seq 1, EHZ seq 2).
            REQUIRE(store.truncateOldest(2) == 2);
            // The max is preserved - only the old tail was trimmed.
            REQUIRE(store.getGlobalSequenceNumber() == 5);
            const auto hhz = store.query({{::makeIdentifier("HHZ"), 0}});
            REQUIRE(hhz.size() == 2);
            REQUIRE(hhz[0].sequenceNumber == 3);
            const auto ehz = store.query({{::makeIdentifier("EHZ"), 0}});
            REQUIRE(ehz.size() == 1);
            REQUIRE(ehz[0].sequenceNumber == 4);
            // Both channels still have data, so both survive.
            REQUIRE(store.queryAvailableStreams().size() == 2);

            // Purge the remainder: registry empties, streams are reaped.
            REQUIRE(store.truncateOldest(1000) == 3);
            REQUIRE(store.queryAvailableStreams().empty());
            REQUIRE(store.getGlobalSequenceNumber() == 0);

            // Nothing left - returns 0 so the monitor loop stops.
            REQUIRE(store.truncateOldest(256) == 0);
        }
    }

    SECTION("State survives a reopen")
    {
        RocksDatabaseOptions reopenOptions;
        reopenOptions.setDatabaseDirectory(databasePath);  // do not overwrite
        RocksDatabase reopened(reopenOptions, nullptr);
        REQUIRE(reopened.isInitialized());
        REQUIRE(reopened.getGlobalSequenceNumber() == 5);
        REQUIRE(reopened.queryAvailableStreams().size() == 2);
        const auto responses
            = reopened.query({{::makeIdentifier("HHZ"), 1}});
        REQUIRE(responses.size() == 3);
    }

    std::filesystem::remove_all(databasePath);
}
