#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <utility>
#include "uDataPacketBroker/rocksDatabaseOptions.hpp"

using namespace UDataPacketBroker;

class RocksDatabaseOptions::RocksDatabaseOptionsImpl
{
public:
    std::filesystem::path mDatabaseDirectory;
    std::chrono::seconds mRetention{std::chrono::hours {1}};
    size_t mWriteBufferSizeInBytes{static_cast<size_t> (128) << 20};
    int mMaximumNumberOfMemoryTables{2};
    bool mEnableWriteAheadLog{false};
    bool mOverWriteIfExists{false};
};

/// Constructor
RocksDatabaseOptions::RocksDatabaseOptions() :
    pImpl(std::make_unique<RocksDatabaseOptionsImpl> ())
{
}

/// Copy constructor
RocksDatabaseOptions::RocksDatabaseOptions(
    const RocksDatabaseOptions &options)
{
    *this = options;
}

/// Move constructor
RocksDatabaseOptions::RocksDatabaseOptions(
    RocksDatabaseOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
RocksDatabaseOptions&
RocksDatabaseOptions::operator=(const RocksDatabaseOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<RocksDatabaseOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
RocksDatabaseOptions&
RocksDatabaseOptions::operator=(RocksDatabaseOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Database directory
void RocksDatabaseOptions::setDatabaseDirectory(
    const std::filesystem::path &databaseDirectory)
{
    if (databaseDirectory.empty())
    {
        throw std::invalid_argument("Database directory is empty");
    }
    pImpl->mDatabaseDirectory = databaseDirectory;
}

std::filesystem::path RocksDatabaseOptions::getDatabaseDirectory() const
{
    if (!hasDatabaseDirectory())
    {
        throw std::runtime_error("Database directory not set");
    }
    return pImpl->mDatabaseDirectory;
}

bool RocksDatabaseOptions::hasDatabaseDirectory() const noexcept
{
    return !pImpl->mDatabaseDirectory.empty();
}

/// Overwrite if exists
void RocksDatabaseOptions::enableOverWriteIfExists() noexcept
{
    pImpl->mOverWriteIfExists = true;
}

void RocksDatabaseOptions::disableOverWriteIfExists() noexcept
{
    pImpl->mOverWriteIfExists = false;
}

bool RocksDatabaseOptions::overWriteIfExists() const noexcept
{
    return pImpl->mOverWriteIfExists;
}

/// Retention
void RocksDatabaseOptions::setRetention(const std::chrono::seconds &retention)
{
    if (retention.count() <= 0)
    {
        throw std::invalid_argument("Retention must be positive");
    }
    pImpl->mRetention = retention;
}

std::chrono::seconds RocksDatabaseOptions::getRetention() const noexcept
{
    return pImpl->mRetention;
}

/// Write buffer size
void RocksDatabaseOptions::setWriteBufferSizeInBytes(
    const size_t writeBufferSizeInBytes)
{
    if (writeBufferSizeInBytes == 0)
    {
        throw std::invalid_argument("Write buffer size must be positive");
    }
    pImpl->mWriteBufferSizeInBytes = writeBufferSizeInBytes;
}

size_t RocksDatabaseOptions::getWriteBufferSizeInBytes() const noexcept
{
    return pImpl->mWriteBufferSizeInBytes;
}

/// Maximum number of memory tables
void RocksDatabaseOptions::setMaximumNumberOfMemoryTables(
    const int maximumNumberOfMemoryTables)
{
    if (maximumNumberOfMemoryTables < 1)
    {
        throw std::invalid_argument(
            "Maximum number of memory tables must be positive");
    }
    pImpl->mMaximumNumberOfMemoryTables = maximumNumberOfMemoryTables;
}

int RocksDatabaseOptions::getMaximumNumberOfMemoryTables() const noexcept
{
    return pImpl->mMaximumNumberOfMemoryTables;
}

/// Write-ahead-log
void RocksDatabaseOptions::disableWriteAheadLog() noexcept
{
    pImpl->mEnableWriteAheadLog = false;
}

void RocksDatabaseOptions::enableWriteAheadLog() noexcept
{
    pImpl->mEnableWriteAheadLog = true;
}

bool RocksDatabaseOptions::useWriteAheadLog() const noexcept
{
    return pImpl->mEnableWriteAheadLog;
}

/// Destructor
RocksDatabaseOptions::~RocksDatabaseOptions() = default;
