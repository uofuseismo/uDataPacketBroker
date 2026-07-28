#ifndef UDATA_PACKET_BROKER_DATA_PACKET_STORE_HPP
#define UDATA_PACKET_BROKER_DATA_PACKET_STORE_HPP
#include <chrono>
#include <string>
#include <utility>
#include <vector>
namespace UDataPacketBrokerAPI::V1
{
 class Packet;
 class StreamIdentifier;
} 
namespace UDataPacketBroker
{
/// @class DataPacketStore dataPacketStore.hpp
/// @brief A data packet store is a utility that can store and query saved
///        data packets.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class IDataPacketStore 
{
public:
    struct QueryResponse
    {
        /// The packet loaded from Rocks.  Note, this is a container for
        /// the raw bytes created with packet.SerializeToString(). 
        /// The data packet can be reconstituted afterwards with
        /// ParseFromArray.
        std::string packet;
        /// The time the packet was received (debugging).
        std::chrono::nanoseconds receivedAt;
        /// The sequence number for the subscriber.
        uint64_t sequenceNumber;
    };
    /// @brief Defines the API packet version should I ever upgrade the API.
    enum class PacketVersion
    {
        One = 1 
    }; 
public:
    /// @brief On startup the broker uses this to get the global
    ///        sequence number that was last written prior to 
    ///        shutdown.  It begins iterating at the next 
    ///        number.
    /// @throws std::runtime_error if \c isInitialized() is false.
    [[nodiscard]] virtual uint64_t getGlobalSequenceNumber() const = 0;

    /// @name Used By Producer Thread
    /// { 

    /// @brief Writes a single packet.
    /// @param[in] receiptTimeAndData  The time the packet was received
    ///                                and the payload.  Note, the calling
    ///                                thread has updated the packet's 
    ///                                sequence number prior to writing.
    /// @result True indicates the packet was written.
    /// @throws std::runtime_error if \c isInitialized() is false.
    [[nodiscard]] virtual bool write(const std::pair<std::chrono::nanoseconds, UDataPacketBrokerAPI::V1::Packet> &receiptTimeAnData);
    [[nodiscard]] virtual bool write(std::pair<std::chrono::nanoseconds, UDataPacketBrokerAPI::V1::Packet> &&receiptTimeAndData);
    /// @brief Writes a collection of packets.
    /// @param[in] receiptTimesAndData  The times the packets were received
    ///                                 and the payload.  Note, the calling
    ///                                 thread has updated the packet's
    ///                                 sequence numbers prior to writing.
    /// @result The packets that were not successfully written.
    /// @note If certain packets were not successfully written then it
    ///       is possible to retry.
    /// @throws std::runtime_error if \c isInitialized() is false.
    [[nodiscard]] virtual std::vector<UDataPacketBrokerAPI::V1::Packet>
        write(const std::vector<std::pair<std::chrono::nanoseconds, UDataPacketBrokerAPI::V1::Packet>> &receiptTimesAndData) = 0;
    /// @}

    /// @name Used By Monitor Thread
    /// @{

    /// @brief Removes packets acquired before a given time.
    /// @param[in] dropBefore   Packets acquired before this time.
    virtual void compact(const std::chrono::nanoseconds &dropBefore) = 0;
    /// @brief Flushes any buffered (in-memory) packets to durable storage.
    ///        The default does nothing; a store that buffers writes in memory
    ///        overrides this so a graceful shutdown does not lose data.
    virtual void flush();

    /// @result The size of the store in bytes.  This is the figure a
    ///         size-bounded deployment is measured against - a persistent
    ///         volume claim's quota for an on-disk store, or the memory request
    ///         for an in-memory store.  Every store must implement this.
    [[nodiscard]] virtual size_t getSizeInBytes() const = 0;

    /// @brief Purges (approximately) the oldest \c nPackets packets and
    ///        reclaims their space.  A monitor thread loops this while
    ///        \c getSizeInBytes() exceeds a safe limit, so the implementation
    ///        MUST actually free the space here such that a subsequent
    ///        \c getSizeInBytes() reflects the removal - for an on-disk store
    ///        (e.g., RocksDB) that means compacting after deleting; for a pure
    ///        in-memory store it is a trivial drop from the front.
    /// @param[in] nPackets  The number of oldest packets to purge.
    /// @result The number of packets actually purged.  This is 0 when there is
    ///         nothing left to purge, which the caller uses to stop looping so
    ///         it never spins when no progress can be made.
    [[nodiscard]] virtual size_t truncateOldest(size_t nPackets) = 0;
    /// @}

    /// @name Used By Consumer Thread(s)
    /// @{

    /// @brief Queries the packets corresonding to this identifier
    ///        beginning at (and including) the given sequence number.
    /// @param[in] identifierAndSequenceNumber  The stream identifier and
    ///                       the number at which to begin retaining 
    ///                       matching packets.
    /// @param[in] sequenceNumber  The sequence at which to begin
    ///                            retaining packets.
    /// @result The response to the query.
    [[nodiscard]] virtual std::vector<QueryResponse>
         query(const std::pair<UDataPacketBrokerAPI::V1::StreamIdentifier, uint64_t> &identifierAndSequenceNumber) const;
    /// @brief Queries the packets corresonding to these identifiers
    ///        beginning at (and including) the given sequence numbers.
    /// @param[in] identifierAndSequenceNumber  The stream identifier and
    ///                       the number at which to begin retaining 
    ///                       matching packets.
    /// @param[in] sequenceNumber  The sequence at which to begin
    ///                            retaining packets.
    /// @result The response to the query.
    [[nodiscard]] virtual std::vector<QueryResponse> 
        query(const std::vector<std::pair<UDataPacketBrokerAPI::V1::StreamIdentifier, uint64_t>> identifiersAndSequenceNumbers) const = 0;

    /// @result The streams currently available in the database and when 
    ///         a packet from that stream was last received.
    [[nodiscard]] virtual std::vector<std::pair<std::chrono::nanoseconds, UDataPacketBrokerAPI::V1::StreamIdentifier>> queryAvailableStreams() const = 0;
    /// @}

    /// @brief Destructor.
    virtual ~IDataPacketStore();
};
}
#endif
