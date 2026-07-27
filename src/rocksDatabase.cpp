#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include <rocksdb/db.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/write_batch.h>
#include <uDataPacketBrokerAPI/v1/packet.pb.h>
#include <uDataPacketBrokerAPI/v1/stream_identifier.pb.h>
#include "uDataPacketBroker/rocksDatabase.hpp"
#include "uDataPacketBroker/rocksDatabaseOptions.hpp"
#include "uDataPacketBroker/dataPacketStore.hpp"
#include "uDataPacketBroker/utilities.hpp"

using namespace UDataPacketBroker;

namespace
{

using Packet = UDataPacketBrokerAPI::V1::Packet;
using StreamIdentifier = UDataPacketBrokerAPI::V1::StreamIdentifier;

/// The column family holding one entry per packet keyed net|sta|loc|chan|seq.
constexpr const char *packetsColumnFamily{"packets"};
/// The registry column family holding one entry per stream.
constexpr const char *streamsColumnFamily{"streams"};
/// The registry value width: min_seq(8) | max_seq(8) | last_received_ns(8).
constexpr std::size_t streamsValueLength{24};
/// The packet value header width: recv_time_ns(8) | version(1).
constexpr std::size_t packetsValueHeaderLength{9};

/// Appends a big-endian uint64 so that a byte-wise comparison orders numerically.
void putBigEndian64(std::string &out, uint64_t value)
{
    for (int shift = 56; shift >= 0; shift = shift - 8)
    {
        out.push_back(static_cast<char> ((value >> shift) & 0xFFU));
    }
}

/// Reads a big-endian uint64 from eight bytes at the given pointer.
[[nodiscard]]
uint64_t getBigEndian64(const char *data)
{
    uint64_t value{0};
    for (std::size_t i = 0; i < 8; ++i)
    {
        value = (value << 8) | static_cast<unsigned char> (data[i]);
    }
    return value;
}

[[nodiscard]]
std::string_view toStringView(const rocksdb::Slice &slice)
{
    return std::string_view {slice.data(), slice.size()};
}

}

class RocksDatabase::RocksDatabaseImpl
{
public:
    RocksDatabaseImpl(const RocksDatabaseOptions &options,
                      std::shared_ptr<spdlog::logger> logger) :
        mOptions(options),
        mLogger(std::move(logger))
    {
        if (!mOptions.hasDatabase())
        {
            throw std::invalid_argument("Database not set");
        }
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            constexpr const char *loggerName{"RocksDatabaseConsole"};
            mLogger = spdlog::get(loggerName);
            if (mLogger == nullptr)
            {
                mLogger = spdlog::stdout_color_mt(loggerName);
            }
            // NOLINTEND(misc-include-cleaner)
        }
        open();
    }

    // Open the database
    void open()
    {
        const auto databasePath = mOptions.getDatabase();
        if (databasePath.has_parent_path())
        {
            auto parentPath = databasePath.parent_path();
            if (!std::filesystem::exists(parentPath))
            {
                SPDLOG_LOGGER_INFO(mLogger,
                                   "Creating parent path {}",
                                   databasePath.string());
                if (!std::filesystem::create_directories(parentPath))
                {
                    throw std::runtime_error("Failed to create parent path "
                                           + databasePath.string());
                }
            }
        }
        
        if (mOptions.overWriteIfExists() &&
            std::filesystem::exists(databasePath))
        {
            SPDLOG_LOGGER_INFO(mLogger,
                               "Overwriting existing database at {}",
                               databasePath.string());
            rocksdb::DestroyDB(databasePath.string(), rocksdb::Options());
        }

        rocksdb::DBOptions databaseOptions;
        databaseOptions.create_if_missing = true;
        databaseOptions.create_missing_column_families = true;

        const auto retentionSeconds
            = static_cast<uint64_t> (mOptions.getRetention().count());

        // The packets column family: fixed 32-byte channel prefix, prefix
        // bloom filters, and a file-granular ttl that expires whole SSTs.
        rocksdb::ColumnFamilyOptions packetsOptions;
        packetsOptions.write_buffer_size = mOptions.getWriteBufferSize();
        packetsOptions.ttl = retentionSeconds;
        packetsOptions.compaction_pri = rocksdb::kOldestSmallestSeqFirst;
        packetsOptions.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(Utilities::streamKeyLength));
        rocksdb::BlockBasedTableOptions tableOptions;
        tableOptions.filter_policy.reset(
            rocksdb::NewBloomFilterPolicy(10, false));
        tableOptions.whole_key_filtering = false;
        packetsOptions.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(tableOptions));

        // The registry column family: tiny, one entry per stream, same ttl so
        // a silent stream's entry ages out with its packets.
        rocksdb::ColumnFamilyOptions streamsOptions;
        streamsOptions.ttl = retentionSeconds;

        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
        descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                                 rocksdb::ColumnFamilyOptions());
        descriptors.emplace_back(::packetsColumnFamily, packetsOptions);
        descriptors.emplace_back(::streamsColumnFamily, streamsOptions);

        std::vector<rocksdb::ColumnFamilyHandle *> handles;
        std::unique_ptr<rocksdb::DB> database;
        const auto status = rocksdb::DB::Open(databaseOptions,
                                              databasePath.string(),
                                              descriptors,
                                              &handles,
                                              &database);
        if (!status.ok())
        {
            SPDLOG_LOGGER_ERROR(mLogger,
                                "Failed to open database at {} because {}",
                                databasePath.string(),
                                status.ToString());
            return;
        }
        mDatabase = std::move(database);
        mDefaultHandle = handles.at(0);
        mPacketsHandle = handles.at(1);
        mStreamsHandle = handles.at(2);
        mDisableWriteAheadLog = !mOptions.useWriteAheadLog();
        mInitialized = true;
        SPDLOG_LOGGER_INFO(mLogger, "Opened database at {}",
                           databasePath.string());
    }

    ~RocksDatabaseImpl()
    {
        if (mDatabase)
        {
            if (mDefaultHandle)
            {
                mDatabase->DestroyColumnFamilyHandle(mDefaultHandle);
            }
            if (mPacketsHandle)
            {
                mDatabase->DestroyColumnFamilyHandle(mPacketsHandle);
            }
            if (mStreamsHandle)
            {
                mDatabase->DestroyColumnFamilyHandle(mStreamsHandle);
            }
            mDatabase.reset();
        }
    }

    void ensureInitialized() const
    {
        if (!mInitialized)
        {
            throw std::runtime_error("RocksDatabase is not initialized");
        }
    }

    [[nodiscard]] uint64_t getGlobalSequenceNumber() const
    {
        ensureInitialized();
        uint64_t globalSequenceNumber{0};
        const rocksdb::ReadOptions readOptions;
        std::unique_ptr<rocksdb::Iterator> iterator(
            mDatabase->NewIterator(readOptions, mStreamsHandle));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
        {
            const auto value = iterator->value();
            if (value.size() >= ::streamsValueLength)
            {
                // max_seq lives in bytes [8, 16).
                const auto maxSequenceNumber
                    = ::getBigEndian64(value.data() + 8);
                globalSequenceNumber
                    = std::max(globalSequenceNumber, maxSequenceNumber);
            }
        }
        return globalSequenceNumber;
    }

    [[nodiscard]] std::vector<Packet>
        write(const std::vector<
                 std::pair<std::chrono::nanoseconds, Packet>>
                     &receiptTimeAndData)
    {
        ensureInitialized();
        std::vector<Packet> notWritten;
        if (receiptTimeAndData.empty())
        {
            return notWritten;
        }

        struct Aggregate
        {
            uint64_t minSequenceNumber;
            uint64_t maxSequenceNumber;
            int64_t lastReceivedNanoSeconds;
        };
        std::unordered_map<std::string, Aggregate> streamAggregate;

        rocksdb::WriteBatch batch;
        for (const auto &[receiptTime, packet] : receiptTimeAndData)
        {
            const auto &identifier = packet.stream_identifier();
            const auto sequenceNumber = packet.sequence_number();
            auto packetKey
                = Utilities::toPacketKey(identifier, sequenceNumber);
            if (!packetKey)
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                   "Cannot build key for packet because {}",
                                   packetKey.error());
                notWritten.push_back(packet);
                continue;
            }
            auto streamKey = Utilities::toStreamKey(identifier);

            const auto receiptNanoSeconds
                = static_cast<int64_t> (receiptTime.count());
            // Value layout: recv_time_ns(8) | version(1) | serialized packet.
            // Serialize the live packet once - no parse round trip.
            std::string value;
            value.reserve(::packetsValueHeaderLength + packet.ByteSizeLong());
            ::putBigEndian64(value,
                             static_cast<uint64_t> (receiptNanoSeconds));
            value.push_back(static_cast<char> (
                static_cast<int> (IDataPacketStore::PacketVersion::One)));
            packet.AppendToString(&value);
            batch.Put(mPacketsHandle, *packetKey, value);

            auto [iterator, inserted]
                = streamAggregate.try_emplace(
                     *streamKey,
                     Aggregate{sequenceNumber,
                               sequenceNumber,
                               receiptNanoSeconds});
            if (!inserted)
            {
                iterator->second.minSequenceNumber
                    = std::min(iterator->second.minSequenceNumber,
                               sequenceNumber);
                iterator->second.maxSequenceNumber
                    = std::max(iterator->second.maxSequenceNumber,
                               sequenceNumber);
                iterator->second.lastReceivedNanoSeconds
                    = std::max(iterator->second.lastReceivedNanoSeconds,
                               receiptNanoSeconds);
            }
        }

        // Merge the per-stream aggregates with the existing registry entries.
        // Safe to read-then-write because there is a single writer thread.
        const rocksdb::ReadOptions readOptions;
        for (const auto &[streamKey, aggregate] : streamAggregate)
        {
            uint64_t minSequenceNumber{aggregate.minSequenceNumber};
            uint64_t maxSequenceNumber{aggregate.maxSequenceNumber};
            int64_t lastReceivedNanoSeconds{aggregate.lastReceivedNanoSeconds};
            std::string existing;
            const auto status
                = mDatabase->Get(readOptions, mStreamsHandle,
                                 streamKey, &existing);
            if (status.ok() && existing.size() >= ::streamsValueLength)
            {
                minSequenceNumber = std::min(minSequenceNumber,
                                             ::getBigEndian64(existing.data()));
                maxSequenceNumber
                    = std::max(maxSequenceNumber,
                               ::getBigEndian64(existing.data() + 8));
                lastReceivedNanoSeconds
                    = std::max(lastReceivedNanoSeconds,
                               static_cast<int64_t>
                                   (::getBigEndian64(existing.data() + 16)));
            }
            std::string streamValue;
            ::putBigEndian64(streamValue, minSequenceNumber);
            ::putBigEndian64(streamValue, maxSequenceNumber);
            ::putBigEndian64(streamValue,
                             static_cast<uint64_t> (lastReceivedNanoSeconds));
            batch.Put(mStreamsHandle, streamKey, streamValue);
        }

        rocksdb::WriteOptions writeOptions;
        writeOptions.disableWAL = mDisableWriteAheadLog;
        const auto status = mDatabase->Write(writeOptions, &batch);
        if (!status.ok())
        {
            SPDLOG_LOGGER_ERROR(mLogger,
                                "Batch write failed because {}",
                                status.ToString());
            // The batch is atomic - nothing was written.  Return everything
            // for the caller to retry.
            notWritten.clear();
            notWritten.reserve(receiptTimeAndData.size());
            for (const auto &receiptTimeAndPacket : receiptTimeAndData)
            {
                notWritten.push_back(receiptTimeAndPacket.second);
            }
        }
        return notWritten;
    }

    void compact(const std::chrono::nanoseconds &dropBefore)
    {
        ensureInitialized();
        // Expiry is ttl-driven: file-granular and keyed on SST creation time,
        // which is receive time.  There is no time in the key, so dropBefore
        // cannot be honored with sub-file precision - this simply forces a
        // compaction so expired SSTs are dropped promptly.
        static_cast<void> (dropBefore);
        SPDLOG_LOGGER_DEBUG(mLogger, "Compacting to apply retention");
        const rocksdb::CompactRangeOptions compactOptions;
        mDatabase->CompactRange(compactOptions, mPacketsHandle,
                                nullptr, nullptr);
        mDatabase->CompactRange(compactOptions, mStreamsHandle,
                                nullptr, nullptr);
    }

    [[nodiscard]] std::vector<IDataPacketStore::QueryResponse>
        query(const std::vector<std::pair<StreamIdentifier, uint64_t>>
                 &identifiersAndSequenceNumbers) const
    {
        ensureInitialized();
        std::vector<IDataPacketStore::QueryResponse> responses;
        rocksdb::ReadOptions readOptions;
        readOptions.prefix_same_as_start = true;
        for (const auto &[identifier, startSequenceNumber]
                 : identifiersAndSequenceNumbers)
        {
            auto prefix = Utilities::toStreamKey(identifier);
            auto seekKey
                = Utilities::toPacketKey(identifier, startSequenceNumber);
            if (!prefix || !seekKey)
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                   "Skipping unkeyable query identifier");
                continue;
            }
            std::unique_ptr<rocksdb::Iterator> iterator(
                mDatabase->NewIterator(readOptions, mPacketsHandle));
            for (iterator->Seek(*seekKey);
                 iterator->Valid();
                 iterator->Next())
            {
                const auto keyView = ::toStringView(iterator->key());
                if (!keyView.starts_with(*prefix))
                {
                    break;
                }
                const auto value = iterator->value();
                if (value.size() < ::packetsValueHeaderLength)
                {
                    continue;
                }
                IDataPacketStore::QueryResponse response;
                response.receivedAt = std::chrono::nanoseconds(
                    static_cast<int64_t> (::getBigEndian64(value.data())));
                response.packet.assign(
                    value.data() + ::packetsValueHeaderLength,
                    value.size() - ::packetsValueHeaderLength);
                // The sequence number is the trailing big-endian uint64 of
                // the key; the identifier is already known (the query input).
                response.sequenceNumber = ::getBigEndian64(
                    keyView.data() + Utilities::streamKeyLength);
                responses.push_back(std::move(response));
            }
        }
        return responses;
    }

    [[nodiscard]]
    std::vector<std::pair<std::chrono::nanoseconds, StreamIdentifier>>
        queryAvailableStreams() const
    {
        ensureInitialized();
        std::vector<std::pair<std::chrono::nanoseconds, StreamIdentifier>>
            result;
        const rocksdb::ReadOptions readOptions;
        std::unique_ptr<rocksdb::Iterator> iterator(
            mDatabase->NewIterator(readOptions, mStreamsHandle));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
        {
            StreamIdentifier identifier;
            const auto decoded
                = Utilities::fromStreamKey(::toStringView(iterator->key()),
                                           identifier);
            if (!decoded)
            {
                continue;
            }
            const auto value = iterator->value();
            int64_t lastReceivedNanoSeconds{0};
            if (value.size() >= ::streamsValueLength)
            {
                lastReceivedNanoSeconds = static_cast<int64_t> (
                    ::getBigEndian64(value.data() + 16));
            }
            result.emplace_back(
                std::chrono::nanoseconds(lastReceivedNanoSeconds),
                std::move(identifier));
        }
        return result;
    }
//private:
    RocksDatabaseOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::unique_ptr<rocksdb::DB> mDatabase{nullptr};
    rocksdb::ColumnFamilyHandle *mDefaultHandle{nullptr};
    rocksdb::ColumnFamilyHandle *mPacketsHandle{nullptr};
    rocksdb::ColumnFamilyHandle *mStreamsHandle{nullptr};
    bool mDisableWriteAheadLog{true};
    bool mInitialized{false};
};

/// Constructor
RocksDatabase::RocksDatabase(const RocksDatabaseOptions &options,
                             std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<RocksDatabaseImpl> (options, std::move(logger)))
{
}

/// Global sequence number
uint64_t RocksDatabase::getGlobalSequenceNumber() const
{
    return pImpl->getGlobalSequenceNumber();
}

/// Initialized?
bool RocksDatabase::isInitialized() const noexcept
{
    return pImpl->mInitialized;
}

/// Write
std::vector<UDataPacketBrokerAPI::V1::Packet> RocksDatabase::write(
    const std::vector<
        std::pair<std::chrono::nanoseconds,
                  UDataPacketBrokerAPI::V1::Packet>> &receiptTimeAndData)
{
    return pImpl->write(receiptTimeAndData);
}

/// Compact
void RocksDatabase::compact(const std::chrono::nanoseconds &dropBefore)
{
    pImpl->compact(dropBefore);
}

/// Query
std::vector<IDataPacketStore::QueryResponse> RocksDatabase::query(
    const std::vector<std::pair<UDataPacketBrokerAPI::V1::StreamIdentifier,
                                uint64_t>> identifiersAndSequenceNumbers) const
{
    return pImpl->query(identifiersAndSequenceNumbers);
}

/// Available streams
std::vector<std::pair<std::chrono::nanoseconds,
                      UDataPacketBrokerAPI::V1::StreamIdentifier>>
    RocksDatabase::queryAvailableStreams() const
{
    return pImpl->queryAvailableStreams();
}

/// Destructor
RocksDatabase::~RocksDatabase() = default;
