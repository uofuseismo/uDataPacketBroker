#include <memory>
#include <utility>
#include <stdexcept>
#include <cstdint>
#include <optional>
#include "uDataPacketBroker/publishServiceOptions.hpp"
#include "uDataPacketBroker/grpcServerOptions.hpp"

using namespace UDataPacketBroker;

class PublishServiceOptions::PublishServiceOptionsImpl
{
public:
    GRPCServerOptions mGRPCOptions;
    int mMaximumPublishers{64};
    uint32_t mMaximumConsecutiveInvalidMessages{16};
    std::optional<int> mMaximumMessageSizeInBytes{std::nullopt};
    bool mHasGRPCOptions{false};
};

/// Constructor
PublishServiceOptions::PublishServiceOptions() :
    pImpl(std::make_unique<PublishServiceOptionsImpl> ())
{
}

/// Copy constructor
PublishServiceOptions::PublishServiceOptions(const PublishServiceOptions &options)
{
    *this = options;
}

/// Move constructor
PublishServiceOptions::PublishServiceOptions(PublishServiceOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
PublishServiceOptions &PublishServiceOptions::operator=(const PublishServiceOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<PublishServiceOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
PublishServiceOptions &PublishServiceOptions::operator=(PublishServiceOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
PublishServiceOptions::~PublishServiceOptions() = default;

/// GRPC options
void PublishServiceOptions::setGRPCOptions(const GRPCServerOptions &options)
{
    pImpl->mGRPCOptions = options;
    pImpl->mHasGRPCOptions = true;
}

GRPCServerOptions PublishServiceOptions::getGRPCOptions() const
{
    if (!hasGRPCOptions()){throw std::runtime_error("gRPC options not set");}
    return pImpl->mGRPCOptions;
}

bool PublishServiceOptions::hasGRPCOptions() const noexcept
{
    return pImpl->mHasGRPCOptions;
}

/// The maximum number of publishers
void PublishServiceOptions::setMaximumNumberOfPublishers(
    const int maxPublishers)
{
    if (maxPublishers < 1)
    {
        throw std::invalid_argument(
           "Maximum number of publishers must be positive");
    }
    pImpl->mMaximumPublishers = maxPublishers;
}

int PublishServiceOptions::getMaximumNumberOfPublishers() const noexcept
{
    return pImpl->mMaximumPublishers;
}

/// Max message size
void PublishServiceOptions::setMaximumMessageSizeInBytes(
    const int maxMessageSize)
{
    if (maxMessageSize < 1)
    {
        throw std::invalid_argument(
           "Maximum message size must be positive");
    }
    pImpl->mMaximumMessageSizeInBytes = std::optional<int> (maxMessageSize);
}

std::optional<int>
    PublishServiceOptions::getMaximumMessageSizeInBytes() const noexcept
{
    return pImpl->mMaximumMessageSizeInBytes;
}

/// Consecutive invalid messages
void PublishServiceOptions::setMaximumConsecutiveInvalidMessages(
    const uint32_t maxInvalidMessages) noexcept
{
    pImpl->mMaximumConsecutiveInvalidMessages = maxInvalidMessages;
}

uint32_t PublishServiceOptions::getMaximumConsecutiveInvalidMessages() const noexcept
{
    return pImpl->mMaximumConsecutiveInvalidMessages;
}
