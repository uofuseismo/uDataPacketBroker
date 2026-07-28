#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <uDataPacketBrokerAPI/v1/packet.pb.h>
#include <uDataPacketBrokerAPI/v1/stream_identifier.pb.h>
#include "uDataPacketBroker/dataPacketStore.hpp"

using namespace UDataPacketBroker;

/// Destructor
IDataPacketStore::~IDataPacketStore() = default;

/// Flush - the default does nothing; buffering stores override this.
void IDataPacketStore::flush()
{
}

/// One-off write
bool IDataPacketStore::write(
    const std::pair
    <
        std::chrono::nanoseconds,
        UDataPacketBrokerAPI::V1::Packet
    > &packet)
{
    auto copy = packet;
    return write(std::move(copy));
}

/// One-off write
bool IDataPacketStore::write(
    std::pair
    <
        std::chrono::nanoseconds,
        UDataPacketBrokerAPI::V1::Packet
    > &&receiptTimeAndData)
{
    const std::vector
    <
        std::pair
        <
            std::chrono::nanoseconds,
            UDataPacketBrokerAPI::V1::Packet
        >
    > receiptTimesAndData{std::move(receiptTimeAndData)};
    return write(receiptTimesAndData).empty() ? false : true;
}

/*
/// Writes packets
std::vector<UDataPacketBrokerAPI::V1::Packet> IDataPacketStore::write(
    const std::vector
    <
        std::pair
        <
            std::chrono::nanoseconds,
            UDataPacketBrokerAPI::V1::Packet
        >
    > &receiptTimeAndData)
{
    if (receiptTimeAndData.empty())
    {
        throw std::invalid_argument("No data to write");
    }
    std::vector
    <
        std::pair
        <
            std::chrono::nanoseconds,
            std::pair<PacketVersion, std::string>
        >
    > workspace;
    workspace.reserve(receiptTimeAndData.size());
    for (auto &receiptTimeAndDataPair : receiptTimeAndData)
    {
        std::string payload;
        receiptTimeAndDataPair.second.SerializeToString(&payload);    
        auto versionPayload
            = std::make_pair(PacketVersion::One, std::move(payload));
        auto newElement
            = std::make_pair
              (
                  receiptTimeAndDataPair.first,
                  std::move(versionPayload)
              );
         workspace.push_back(std::move(newElement));
    }
    return write(std::move(workspace));
}
*/

/// One-off query
std::vector<IDataPacketStore::QueryResponse> IDataPacketStore::query(
    const std::pair<UDataPacketBrokerAPI::V1::StreamIdentifier, uint64_t>
       &identifierAndSequenceNumber) const
{
    const std::vector
    <
        std::pair
        <
            UDataPacketBrokerAPI::V1::StreamIdentifier,
            uint64_t
        >
    > identifiersAndSequenceNumbers{identifierAndSequenceNumber};
    return query(identifiersAndSequenceNumbers);
}
