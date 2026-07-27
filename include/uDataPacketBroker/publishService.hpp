#ifndef UDATA_PACKET_BROKER_PUBLISH_SERVICE_HPP
#define UDATA_PACKET_BROKER_PUBLISH_SERVICE_HPP
#include <spdlog/logger.h>
#include <memory>
#include <functional>
#include <future>
namespace UDataPacketBrokerAPI::V1
{
 class Packet;
}
namespace UDataPacketBroker
{
 class PublishServiceOptions;
}

namespace UDataPacketBroker
{
/// @class PublishService publishService.hpp
/// @brief This is the publish service to which seismic data packets
///        are submitted.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class PublishService
{
public:
    /// @brief Constructor.
    /// @param[in] options   The publish service's options.
    /// @param[in] callback  The callback that allows packets to be propagated
    ///                      by the broker.
    /// @param[in] logger    The logging utility.
    PublishService(const PublishServiceOptions &options,
                   const std::function<void (UDataPacketBrokerAPI::V1::Packet &&)> &callback,
                   std::shared_ptr<spdlog::logger> logger);

    /// @brief Starts the publish service's packet acquisition.
    std::future<void> start();
    /// @brief Stops the publish service and prevents the reading of packets.
    void stop();

    /// @brief Destructor.
    ~PublishService();
    PublishService() = delete;
    PublishService(const PublishService &) = delete;
    PublishService(PublishService &&) noexcept = delete;
    PublishService &operator=(const PublishService &) = delete;
    PublishService &operator=(PublishService &&) noexcept = delete;
private:
    class PublishServiceImpl;
    std::unique_ptr<PublishServiceImpl> pImpl;
};
}
#endif
