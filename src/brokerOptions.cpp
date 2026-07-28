#include <chrono>
#include <memory>
#include <utility>
#include <stdexcept>
#include "uDataPacketBroker/brokerOptions.hpp"
#include "uDataPacketBroker/publishServiceOptions.hpp"
//#include "uDataPacketBroker/subscribeServiceOptions.hpp"
#include "uDataPacketBroker/grpcServerOptions.hpp"

using namespace UDataPacketBroker;

class BrokerOptions::BrokerOptionsImpl
{
public:
    PublishServiceOptions mPublishServiceOptions;
    //SubscribeServiceOptions mSubscribeServiceOptions;
    //std::chrono::milliseconds mPickRetention{std::chrono::minutes {30}};
    int mQueueCapacity{8192};
    bool mHasPublishServiceOptions{false};
    bool mHasSubscribeServiceOptions{false};
};

/// Constructor
BrokerOptions::BrokerOptions() :
    pImpl(std::make_unique<BrokerOptionsImpl> ()) 
{
}

/// Copy constructor
BrokerOptions::BrokerOptions(const BrokerOptions &options)
{
    *this = options;
}

/// Move constructor
BrokerOptions::BrokerOptions(BrokerOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
BrokerOptions &BrokerOptions::operator=(const BrokerOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<BrokerOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
BrokerOptions &BrokerOptions::operator=(BrokerOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
BrokerOptions::~BrokerOptions() = default;

/// Publish service options
void BrokerOptions::setPublishServiceOptions(const PublishServiceOptions &options)
{
// TODO fix here
/*
    if (hasSubscribeServiceOptions())
    {
        const auto thisPort = options.getGRPCOptions().getPort();
        const auto subscribeServicePort
            = getSubscribeServiceOptions().getGRPCOptions().getPort();
        if (thisPort == subscribeSvcPort)
        {
            throw std::invalid_argument(
                "Cannot bind publish service to subscribe service port");
        }
    }
*/
    pImpl->mPublishServiceOptions = options;
    pImpl->mHasPublishServiceOptions = true;
}

PublishServiceOptions BrokerOptions::getPublishServiceOptions() const
{
    if (!hasPublishServiceOptions())
    {
        throw std::runtime_error("Publish service options not set");
    }
    return pImpl->mPublishServiceOptions;
}

bool BrokerOptions::hasPublishServiceOptions() const noexcept
{
    return pImpl->mHasPublishServiceOptions;
}

/// Subscribe service options
/*
void BrokerOptions::setSubscribeServiceOptions(const SubscribeServiceOptions &options)
{
    if (hasPublishServiceOptions())
    {
        const auto thisPort = options.getGRPCOptions().getPort();
        const auto publishSvcPort = getPublishServiceOptions().getGRPCOptions().getPort();
        if (thisPort == publishSvcPort)
        {
            throw std::invalid_argument(
                "Cannot bind subscribe service to publish service port");
        }
    }
    pImpl->mSubscribeServiceOptions = options;
    pImpl->mHasSubscribeServiceOptions = true;
}

SubscribeServiceOptions BrokerOptions::getSubscribeServiceOptions() const
{
    if (!hasSubscribeServiceOptions())
    {
        throw std::runtime_error("Subscribe service options not set");
    }
    return pImpl->mSubscribeServiceOptions;
}
*/

bool BrokerOptions::hasSubscribeServiceOptions() const noexcept
{
    return pImpl->mHasSubscribeServiceOptions;
}

