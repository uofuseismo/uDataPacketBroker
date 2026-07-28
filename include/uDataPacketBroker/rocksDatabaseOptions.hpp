#ifndef UDATA_PACKET_BROKER_ROCKS_DATABASE_OPTIONS_HPP
#define UDATA_PACKET_BROKER_ROCKS_DATABASE_OPTIONS_HPP
#include <chrono>
#include <filesystem>
#include <memory>
namespace UDataPacketBroker
{
/// @class RocksDatabaseOptions rocksDatabaseOptions.hpp
/// @brief Defines the options controlling the RocksDB-backed data packet store.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class RocksDatabaseOptions
{
public:
    /// @brief Constructor.
    RocksDatabaseOptions();
    /// @brief Copy constructor.
    RocksDatabaseOptions(const RocksDatabaseOptions &options);
    /// @brief Move constructor.
    RocksDatabaseOptions(RocksDatabaseOptions &&options) noexcept;

    /// @brief The directory in which the database lives.  RocksDB stores the
    ///        database as a directory of files (SSTs, MANIFEST, WAL, ...), not
    ///        a single file - so this is a directory, not a file path.  It is
    ///        created if it does not exist.
    /// @param[in] databaseDirectory  The directory the database lives in.
    /// @throws std::invalid_argument if the directory is empty.
    void setDatabaseDirectory(const std::filesystem::path &databaseDirectory);
    /// @result The directory in which the database lives.
    /// @throws std::runtime_error if \c hasDatabaseDirectory() is false.
    [[nodiscard]] std::filesystem::path getDatabaseDirectory() const;
    /// @result True indicates the database directory was set.
    [[nodiscard]] bool hasDatabaseDirectory() const noexcept;

    /// @brief If the database file exists then it will be leveled and the
    ///        database rebuilt from scratch.
    void enableOverWriteIfExists() noexcept;
    /// @brief If the database exists then it will be loaded and we will
    ///        commence from where we last began.
    void disableOverWriteIfExists() noexcept; 
    /// @result True indicates the database will be overwritten if it exists.
    ///         The default is false.
    [[nodiscard]] bool overWriteIfExists() const noexcept;

    /// @brief Sets the retention time - i.e., roughly how long a packet is kept
    ///        before it is expired.  Expiry is file-granular so this is coarse.
    /// @param[in] retention  The retention duration.
    /// @throws std::invalid_argument if this is not positive.
    void setRetention(const std::chrono::seconds &retention);
    /// @result The retention duration.  By default this is one hour.
    [[nodiscard]] std::chrono::seconds getRetention() const noexcept;

    /// @brief Sets the write buffer (memtable) size in bytes.  Larger buffers
    ///        mean fewer, bigger flushes.
    /// @param[in] writeBufferSizeInBytes  The write buffer size in bytes.
    /// @throws std::invalid_argument if this is not positive.
    void setWriteBufferSizeInBytes(size_t writeBufferSizeInBytes);
    /// @result The write buffer size in bytes.  By default this is 128 MB.
    [[nodiscard]] size_t getWriteBufferSizeInBytes() const noexcept;

    /// @brief Sets the maximum number of in-memory write buffers (memtables)
    ///        that may accumulate before being flushed to disk.  Peak memtable
    ///        memory is roughly getWriteBufferSizeInBytes() times this, so
    ///        bound it to stay within a Kubernetes memory request.
    /// @param[in] maximumNumberOfMemoryTables  The maximum number of memtables.
    /// @throws std::invalid_argument if this is less than one.
    void setMaximumNumberOfMemoryTables(int maximumNumberOfMemoryTables);
    /// @result The maximum number of in-memory write buffers.  By default this
    ///         is 2.
    [[nodiscard]] int getMaximumNumberOfMemoryTables() const noexcept;

    /// @brief Disables the write-ahead-log on the write path.  Disabling
    ///        the WAL is substantially faster but a crash can lose the
    ///        packets between the last flush and the crash.  Data lost this
    ///        way cannot be re-requested by a consumer of the broker.
    void disableWriteAheadLog() noexcept;
    /// @brief Enables the write ahead log.  This is safer but slower.
    void enableWriteAheadLog() noexcept;
    /// @result True indicates we will use a write-ahead-log.
    /// @note The default is false - i.e., do not use a WAL.
    [[nodiscard]] bool useWriteAheadLog() const noexcept;

    /// @brief Destructor.
    ~RocksDatabaseOptions();
    /// @brief Copy assignment operator.
    RocksDatabaseOptions& operator=(const RocksDatabaseOptions &options);
    /// @brief Move assignment operator.
    RocksDatabaseOptions& operator=(RocksDatabaseOptions &&options) noexcept;
private:
    class RocksDatabaseOptionsImpl;
    std::unique_ptr<RocksDatabaseOptionsImpl> pImpl;
};
}
#endif
