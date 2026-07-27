#ifndef UDATA_PACKET_BROKER_ROCKS_DATABASE_HPP
#define UDATA_PACKET_BROKER_ROCKS_DATABASE_HPP
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <spdlog/logger.h>
#include <uDataPacketBroker/dataPacketStore.hpp>
namespace UDataPacketBrokerAPI::V1
{
 class Packet;
 class StreamIdentifier;
} 
namespace UDataPacketBroker
{
 class RocksDatabaseOptions;  
}
namespace UDataPacketBroker
{
/// @class RocksDatabase database.hpp
/// @brief Manages interaction with the RocksDB database.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class RocksDatabase final : public IDataPacketStore
{
public:
    /// @brief Constructs the database from the given options.
    /// @param[in] options   The RocksDB options.
    /// @param[in] logger    The application logger.
    RocksDatabase(const RocksDatabaseOptions &options,
                  std::shared_ptr<spdlog::logger> logger);

    /// @brief On startup the broker uses this to get the global
    ///        sequence number that was last written prior to 
    ///        shutdown.  It begins iterating at the next 
    ///        number.
    /// @throws std::runtime_error if \c isInitialized() is false.
    [[nodiscard]] uint64_t getGlobalSequenceNumber() const final;
    /// @result True indicates the class is initialized.
    [[nodiscard]] bool isInitialized() const noexcept;

    /// @name Used By Producer Thread
    /// { 

    /// @brief Writes a single packet.
    /// @param[in] receiptTimeAndData  The time the packet was received
    ///                                and the payload.  Note, the calling
    ///                                thread has updated the packet's 
    ///                                sequence number prior to writing.
    /// @result True indicates the packet was written.
    /// @throws std::runtime_error if \c isInitialized() is false.
//    [[nodiscard]] bool write(const std::pair<std::chrono::nanoseconds, UDataPacketBrokerAPI::V1::Packet> &packet);
    /// @brief Writes a collection of packets.
    /// @param[in] receiptTimeAndData  The time the packet was received
    ///                                and payload.  Note, the calling
    ///                                thread has updated the packet's
    ///                                sequence numbers prior to writing.
    /// @result The packets that were not successfully written.
    /// @note If certain packets were not successfully written then it
    ///       is possible to retry.
    /// @throws std::runtime_error if \c isInitialized() is false.
    [[nodiscard]] std::vector<UDataPacketBrokerAPI::V1::Packet>
        write(const std::vector<
                 std::pair
                 <
                     std::chrono::nanoseconds,
                     UDataPacketBrokerAPI::V1::Packet
                 >
              > &receiptTimeAndData) final;
    /// @}

    /// @name Used By Monitor Thread
    /// @{

    /// @brief Removes packets acquired before a given time.
    /// @param[in] dropBefore   Packets acquired before this time.
    void compact(const std::chrono::nanoseconds &dropBefore) final;
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
//    [[nodiscard]] std::vector<QueryResponse>
//         query(const std::pair<UDataPacketBrokerAPI::V1::StreamIdentifier, uint64_t> &identifierAndSequenceNumber) const;
    /// @brief Queries the packets corresonding to these identifiers
    ///        beginning at (and including) the given sequence numbers.
    /// @param[in] identifierAndSequenceNumber  The stream identifier and
    ///                       the number at which to begin retaining 
    ///                       matching packets.
    /// @param[in] sequenceNumber  The sequence at which to begin
    ///                            retaining packets.
    /// @result The response to the query.
    [[nodiscard]] std::vector<IDataPacketStore::QueryResponse> 
        query(const std::vector<std::pair<UDataPacketBrokerAPI::V1::StreamIdentifier, uint64_t>> identifiersAndSequenceNumbers) const final;

    /// @result The streams currently available in the database and when 
    ///         a packet from that stream was last received.
    [[nodiscard]] std::vector<std::pair<std::chrono::nanoseconds, UDataPacketBrokerAPI::V1::StreamIdentifier>>
         queryAvailableStreams() const final;
    /// @}

    /// @brief Destructor.
    virtual ~RocksDatabase();

    RocksDatabase(const RocksDatabase &) = delete;
    RocksDatabase(RocksDatabase &&) noexcept = delete;
    RocksDatabase operator=(const RocksDatabase &) = delete;
    RocksDatabase operator=(RocksDatabase &&) noexcept = delete;
private:
    class RocksDatabaseImpl;
    std::unique_ptr<RocksDatabaseImpl> pImpl; 
};
}
#endif
