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
    std::filesystem::path mDatabase;
    std::chrono::seconds mRetention{std::chrono::hours {1}};
    size_t mWriteBufferSize{static_cast<size_t> (128) << 20};
    bool mEnableWriteAheadLog{false};
    bool mOverwriteIfExists{false};
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

/// Database path
void RocksDatabaseOptions::setDatabase(
    const std::filesystem::path &databaseFile)
{
    if (databaseFile.empty())
    {
        throw std::invalid_argument("Database file is empty");
    }
    pImpl->mDatabase = databaseFile;
}

std::filesystem::path RocksDatabaseOptions::getDatabase() const
{
    if (!hasDatabase())
    {
        throw std::runtime_error("Database not set");
    }
    return pImpl->mDatabase;
}

bool RocksDatabaseOptions::hasDatabase() const noexcept
{
    return !pImpl->mDatabase.empty();
}

/// Overwrite if exists
void RocksDatabaseOptions::enableOverwWriteIfExists() noexcept
{
    pImpl->mOverwriteIfExists = true;
}

void RocksDatabaseOptions::disableOverwWiteIfExists() noexcept
{
    pImpl->mOverwriteIfExists = false;
}

bool RocksDatabaseOptions::overWriteIfExists() const noexcept
{
    return pImpl->mOverwriteIfExists;
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
void RocksDatabaseOptions::setWriteBufferSize(const size_t writeBufferSize)
{
    if (writeBufferSize == 0)
    {
        throw std::invalid_argument("Write buffer size must be positive");
    }
    pImpl->mWriteBufferSize = writeBufferSize;
}

size_t RocksDatabaseOptions::getWriteBufferSize() const noexcept
{
    return pImpl->mWriteBufferSize;
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
