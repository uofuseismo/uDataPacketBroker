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

    /// @brief The absolute path to the database.
    /// @param[in] databaseFile  The full path to the database file.  If the
    ///                          file does not exist then the database will
    ///                          be created.
    /// @throws std::invalid_argument if the database file is empty.
    void setDatabase(const std::filesystem::path &databaseFile);
    /// @result The path to the database.
    /// @throws std::runtime_error if \c hasDatabase() is false.
    [[nodiscard]] std::filesystem::path getDatabase() const;
    /// @result True indicates the database was set.
    [[nodiscard]] bool hasDatabase() const noexcept;

    /// @brief If the database file exists then it will be leveled and the
    ///        database rebuilt from scratch.
    void enableOverwWriteIfExists() noexcept;
    /// @brief If the database exists then it will be loaded and we will
    ///        commence from where we last began.
    void disableOverwWiteIfExists() noexcept; 
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
    /// @param[in] writeBufferSize  The write buffer size in bytes.
    /// @throws std::invalid_argument if this is not positive.
    void setWriteBufferSize(size_t writeBufferSize);
    /// @result The write buffer size in bytes.  By default this is 128 MB.
    [[nodiscard]] size_t getWriteBufferSize() const noexcept;

    /// @brief Disables the write-ahead-log on the write path.  Disabling
    ///        the WAL is substantially faster but a crash can lose the
    ///        packets between the last flush and the crash.  Data lost this
    ///        way cannot be re-requested.
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
