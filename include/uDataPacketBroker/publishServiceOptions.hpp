#ifndef UDATA_PACKET_BROKER_PUBLISH_SERVICE_OPTIONS_HPP
#define UDATA_PACKET_BROKER_PUBLISH_SERVICE_OPTIONS_HPP
#include <memory>
#include <optional>
namespace UDataPacketBroker
{
 class GRPCServerOptions;
}
namespace UDataPacketBroker
{
/// @class PublishServiceOptions publishServiceOptions.hpp
/// @brief Defines the publish service's options.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class PublishServiceOptions
{
public:
    /// @brief Constructor.
    PublishServiceOptions();
    /// @brief Copy constructor.
    PublishServiceOptions(const PublishServiceOptions &options);
    /// @brief Move constructor.
    PublishServiceOptions(PublishServiceOptions &&options) noexcept;

    /// @brief Sets the gRPC server options.
    /// @param[in] options  The gRPC server options.
    void setGRPCOptions(const GRPCServerOptions &options);
    /// @result The gRPC server options.
    /// @throws std::runtime_error if \c hasGRPCOptions() is false.
    [[nodiscard]] GRPCServerOptions getGRPCOptions() const;
    /// @result True indicates the gRPC options were set.
    [[nodiscard]] bool hasGRPCOptions() const noexcept;

    /// @brief Sets the maximum number of publishers.
    /// @param[in] maxPublishers  The maximum number of publishers.
    /// @throws std::invalid_argument if this is not positive.
    void setMaximumNumberOfPublishers(int maxPublishers);
    /// @result The maximum number of publishers.
    [[nodiscard]] int getMaximumNumberOfPublishers() const noexcept;

    /// @brief Sets the maximum message size in bytes.
    /// @param[in] maxMessageSize  The maximum message size.
    /// @throws std::invalid_argument if this is not positive.
    void setMaximumMessageSizeInBytes(int maxMessageSize);
    /// @result The maximum message size in bytes.
    [[nodiscard]] std::optional<int> getMaximumMessageSizeInBytes() const noexcept;

    /// @brief Sets the maximum number of consecutive invalid messages before
    ///        the client is bounced with a 400 error.
    void setMaximumConsecutiveInvalidMessages(uint32_t maxConsecutiveInvalidMessages) noexcept;
    /// @result After submitting this many invalid messages consecutively
    ///         the client is purged.
    /// @note By default this is 8.
    [[nodiscard]] uint32_t getMaximumConsecutiveInvalidMessages() const noexcept;

    /// @brief Destructor.
    ~PublishServiceOptions();
    /// @brief Copy assignment.
    PublishServiceOptions& operator=(const PublishServiceOptions &options);
    /// @brief Move assignment.
    PublishServiceOptions& operator=(PublishServiceOptions &&options) noexcept;
private:
    class PublishServiceOptionsImpl;
    std::unique_ptr<PublishServiceOptionsImpl> pImpl;
};
}
#endif
