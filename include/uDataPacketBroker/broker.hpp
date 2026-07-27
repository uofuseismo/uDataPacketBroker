#ifndef UDATA_PACKET_BROKER_BROKER_HPP
#define UDATA_PACKET_BROKER_BROKER_HPP
#include <chrono>
#include <memory>
#include <spdlog/logger.h>
namespace UDataPacketBroker
{
 class BrokerOptions;
 class IDataPacketStore;
}
namespace UDataPacketBroker
{ 
/// @class Broker broker.hpp
/// @brief This is the application middleware.  It basically does all the work.
///        It receives data from the publisher clients and propagates them to the
///        subscriber clients whilst updating the underlying data store.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class Broker
{
public:
    /// @brief Initializes the broker from the given options, store, and logger.
    Broker(const BrokerOptions &options,
           std::shared_ptr<IDataPacketStore> store,
           std::shared_ptr<spdlog::logger> logger);

    /// @result True indicates the broker is initialized.
    [[nodiscard]] bool isInitialized() const noexcept;

    /// @brief Starts the broker.
    /// @note You only get one shot.
    void start();

    /// @brief Management routine to check the running threads in the broker.
    [[nodiscard]] bool checkFutures(const std::chrono::milliseconds &timeOut = std::chrono::milliseconds {10}) const noexcept;

    /// @brief Stops the broker.
    void stop();

    /// @brief Destructor.
    ~Broker();

    Broker(const Broker &) = delete;
    Broker(Broker &&) noexcept = delete;
    Broker& operator=(const Broker &broker) = delete;
    Broker& operator=(Broker &&broker) noexcept = delete;    
private:
    class BrokerImpl;
    std::unique_ptr<BrokerImpl> pImpl;
};
}
#endif
