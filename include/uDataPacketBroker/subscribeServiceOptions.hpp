#ifndef UDATA_PACKET_BROKER_SUBSCRIBE_SERVICE_OPTIONS_HPP
#define UDATA_PACKET_BROKER_SUBSCRIBE_SERVICE_OPTIONS_HPP
#include <memory>
namespace UDataPacketBroker
{
 class GRPCServerOptions;
 //class PacketStoreOptions;
}
namespace UDataPacketBroker
{
class SubscribeServiceOptions
{
public:
    /// @brief Move constructor.
    SubscribeServiceOptions();
    /// @brief Copy constructor.
    SubscribeServiceOptions(const SubscribeServiceOptions &options);
    /// @brief Move constructor.
    SubscribeServiceOptions(SubscribeServiceOptions &&options) noexcept;

    /// @brief Sets the gRPC server options.
    /// @param[in] options  The gRPC server options.
    void setGRPCOptions(const GRPCServerOptions &options);
    /// @result The gRPC server options.
    /// @throws std::runtime_error if \c hasGRPCOptions() is false.
    [[nodiscard]] GRPCServerOptions getGRPCOptions() const;
    /// @result True indicates the gRPC options were set.
    [[nodiscard]] bool hasGRPCOptions() const noexcept;

    /// @param[in] maxSubscribers  The maximum number of subscribers.
    void setMaximumNumberOfSubscribers(const int maxSubscribers);
    /// @result The maximum number of subscribers.
    /// @note By default this is 64.
    [[nodiscard]] int getMaximumNumberOfSubscribers() const noexcept;

    /// @brief Sets the output queue size.
    void setQueueCapacity(int queueCapacity);
    /// @result The maximum output queue size.
    [[nodiscard]] int getQueueCapacity() const noexcept;

    /// @brief Destructor.
    ~SubscribeServiceOptions();
    /// @brief Copy assignment.
    SubscribeServiceOptions& operator=(const SubscribeServiceOptions &options);
    /// @brief Move assignment.
    SubscribeServiceOptions& operator=(SubscribeServiceOptions &&options) noexcept;
private:
    class SubscribeServiceOptionsImpl;
    std::unique_ptr<SubscribeServiceOptionsImpl> pImpl;
};
}
#endif
