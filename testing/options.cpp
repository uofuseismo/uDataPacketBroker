#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <optional>
#include <catch2/catch_test_macros.hpp>
#include "uDataPacketBroker/grpcServerOptions.hpp"
#include "uDataPacketBroker/publishServiceOptions.hpp"
#include "uDataPacketBroker/subscribeServiceOptions.hpp"
#include "uDataPacketBroker/rocksDatabaseOptions.hpp"
//#include "uDataPacketBroker/brokerOptions.hpp"

using namespace UDataPacketBroker;

TEST_CASE("gRPC Server options", "[GRPCServerOptions]")
{
    SECTION("Defaults")
    {
        const GRPCServerOptions options;
        REQUIRE(options.getHost() == "localhost");
        REQUIRE(options.getPort() == 50000);
        REQUIRE(options.getAccessToken() == std::nullopt);
        REQUIRE(options.getServerCertificate() == std::nullopt);
        REQUIRE(options.getServerKey() == std::nullopt);
        REQUIRE(options.getClientCertificate() == std::nullopt);
        REQUIRE(options.isReflectionEnabled() == false);
    }

    SECTION("Options")
    {
        const std::string host{"some.host.org"};
        const std::string token{"super-secret-token"};
        const std::string serverCertificate{"some-wonky-hash"};
        const std::string serverKey{"some-private-wonky-hash"};
        const std::string clientCertificate{"some-rad-hash"};
        constexpr uint16_t port{12345};
        GRPCServerOptions options;

        options.setHost(host);
        options.setPort(port);
        options.setServerCertificate(serverCertificate);
        options.setServerKey(serverKey);
        options.setClientCertificate(clientCertificate);
        options.setAccessToken(token);
        options.enableReflection();

        const GRPCServerOptions copy{options};

        REQUIRE(copy.getHost() == host);
        REQUIRE(copy.getPort() == port);
        REQUIRE(copy.getServerCertificate() != std::nullopt);
        REQUIRE(copy.getServerKey() != std::nullopt);
        REQUIRE(copy.getClientCertificate() != std::nullopt);
        REQUIRE(copy.getAccessToken() != std::nullopt);
        REQUIRE(*copy.getServerCertificate() == serverCertificate); //NOLINT
        REQUIRE(*copy.getServerKey() == serverKey); //NOLINT
        REQUIRE(*copy.getClientCertificate() == clientCertificate); //NOLINT
        REQUIRE(*copy.getAccessToken() == token); //NOLINT
        REQUIRE(copy.isReflectionEnabled() == true);
    }
}

TEST_CASE("Publish service options", "[PublisherServerOptions]")
{
    SECTION("Defaults")
    {
        const PublishServiceOptions options;
        REQUIRE(options.getMaximumMessageSizeInBytes() == std::nullopt);
        REQUIRE(options.getMaximumNumberOfPublishers() == 64);
        REQUIRE(options.getMaximumConsecutiveInvalidMessages() == 16);
        REQUIRE(options.hasGRPCOptions() == false);
    }

    SECTION("Options")
    {
        const std::string host{"some.host.org"};
        constexpr uint16_t port{12345};
        constexpr int maxPublishers{383};
        constexpr int maxMessageSize{83393};
        constexpr uint32_t maxInvalidMessages{38};
        GRPCServerOptions grpcOptions;

        grpcOptions.setHost(host);
        grpcOptions.setPort(port);

        PublishServiceOptions options;
        REQUIRE_NOTHROW(options.setGRPCOptions(grpcOptions));
        REQUIRE_NOTHROW(options.setMaximumNumberOfPublishers(maxPublishers));
        REQUIRE_NOTHROW(options.setMaximumMessageSizeInBytes(maxMessageSize));
        options.setMaximumConsecutiveInvalidMessages(maxInvalidMessages);

        const PublishServiceOptions copy{options};
        REQUIRE(copy.getGRPCOptions().getHost() == host);
        REQUIRE(copy.getGRPCOptions().getPort() == port);
        REQUIRE(copy.getMaximumNumberOfPublishers() == maxPublishers);
        REQUIRE(copy.getMaximumConsecutiveInvalidMessages() == maxInvalidMessages);
        REQUIRE(copy.getMaximumMessageSizeInBytes() != std::nullopt);
        REQUIRE(*copy.getMaximumMessageSizeInBytes() == maxMessageSize); //NOLINT
    }
}

TEST_CASE("Subscribe service options", "[SubscribeServiceOptions]")
{
    SECTION("Defaults")
    {
        const SubscribeServiceOptions options;
        REQUIRE(options.getMaximumNumberOfSubscribers() == 64);
        //REQUIRE(options.getPickStoreOptions().getMaximumQueueCapacity()
        //        == 256);
        //REQUIRE(options.getPickStoreOptions().getMaximumRetentionDuration()
        //        == std::chrono::minutes {30});
        REQUIRE(options.hasGRPCOptions() == false);
    }

    SECTION("Options")
    {
        const std::string host{"another.host.org"};
        constexpr uint16_t port{6432};
        constexpr int maxSubscribers{3833};
        //constexpr int queueCapacity{910};
        //constexpr std::chrono::seconds maxDuration{8383};
        GRPCServerOptions grpcOptions;

        grpcOptions.setHost(host);
        grpcOptions.setPort(port);

        //PickStoreOptions pickStoreOptions;
        //pickStoreOptions.setMaximumQueueCapacity(queueCapacity);
        //pickStoreOptions.setMaximumRetentionDuration(maxDuration);

        SubscribeServiceOptions options;
        REQUIRE_NOTHROW(options.setGRPCOptions(grpcOptions));
        REQUIRE_NOTHROW(options.setMaximumNumberOfSubscribers(maxSubscribers));
        //REQUIRE_NOTHROW(options.setPickStoreOptions(pickStoreOptions));

        const SubscribeServiceOptions copy{options};
        REQUIRE(copy.getGRPCOptions().getHost() == host);
        REQUIRE(copy.getGRPCOptions().getPort() == port);
        REQUIRE(copy.getMaximumNumberOfSubscribers() == maxSubscribers);
        //REQUIRE(copy.getPickStoreOptions().getMaximumQueueCapacity()
        //        == queueCapacity);
        //REQUIRE(copy.getPickStoreOptions().getMaximumRetentionDuration()
        //        == maxDuration);
    }
}

TEST_CASE("RocksDatabase options", "[RocksDatabaseOptions]")
{
    SECTION("Defaults")
    {
        const RocksDatabaseOptions options;
        REQUIRE(options.hasDatabaseDirectory() == false);
        REQUIRE(options.getRetention() == std::chrono::hours {1});
        REQUIRE(options.getWriteBufferSizeInBytes()
                == (static_cast<size_t> (128) << 20));
        REQUIRE(options.getMaximumNumberOfMemoryTables() == 2);
        REQUIRE(options.overWriteIfExists() == false);
        REQUIRE(options.useWriteAheadLog() == false);
    }

    SECTION("Options")
    {
        const std::filesystem::path databaseFile{"/tmp/uDataPacketBroker/db"};
        constexpr std::chrono::seconds retention{7200};
        constexpr size_t writeBufferSize{static_cast<size_t> (64) << 20};
        constexpr int maximumMemoryTables{4};
        RocksDatabaseOptions options;

        // Invalid values are rejected.
        REQUIRE_THROWS(options.setDatabaseDirectory(std::filesystem::path {}));
        REQUIRE_THROWS(options.setRetention(std::chrono::seconds {0}));
        REQUIRE_THROWS(options.setRetention(std::chrono::seconds {-1}));
        REQUIRE_THROWS(options.setWriteBufferSizeInBytes(0));
        REQUIRE_THROWS(options.setMaximumNumberOfMemoryTables(0));
        // Unset database throws on read.
        REQUIRE_THROWS(options.getDatabaseDirectory());

        REQUIRE_NOTHROW(options.setDatabaseDirectory(databaseFile));
        REQUIRE_NOTHROW(options.setRetention(retention));
        REQUIRE_NOTHROW(options.setWriteBufferSizeInBytes(writeBufferSize));
        REQUIRE_NOTHROW(
            options.setMaximumNumberOfMemoryTables(maximumMemoryTables));
        options.enableOverWriteIfExists();
        options.enableWriteAheadLog();

        // Toggles flip both ways.
        options.disableOverWriteIfExists();
        REQUIRE(options.overWriteIfExists() == false);
        options.enableOverWriteIfExists();

        const RocksDatabaseOptions copy{options};
        REQUIRE(copy.hasDatabaseDirectory() == true);
        REQUIRE(copy.getDatabaseDirectory() == databaseFile);
        REQUIRE(copy.getRetention() == retention);
        REQUIRE(copy.getWriteBufferSizeInBytes() == writeBufferSize);
        REQUIRE(copy.getMaximumNumberOfMemoryTables() == maximumMemoryTables);
        REQUIRE(copy.overWriteIfExists() == true);
        REQUIRE(copy.useWriteAheadLog() == true);
    }
}

/*

TEST_CASE("UFilterPickerPickBroker", "[BrokerOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::BrokerOptions options;
        constexpr std::chrono::milliseconds retentionInterval
        {
            std::chrono::minutes {30}
        };
        REQUIRE(options.hasPublishServiceOptions() == false);
        REQUIRE(options.hasSubscribeServiceOptions() == false);
        REQUIRE(options.getQueueCapacity() == 8192);
        REQUIRE(options.getPickRetentionInterval() == retentionInterval);
    }

    SECTION("Options")
    {
        const std::string host{"this.host.org"};
        constexpr uint16_t publishPort{6432};
        constexpr uint16_t subscribePort{6433};
        constexpr int queueCapacity{832};
        constexpr std::chrono::milliseconds 
            retentionInterval{std::chrono::minutes {11}};
        UFilterPickerPickBroker::GRPCServerOptions grpcPublishOptions;
        grpcPublishOptions.setHost(host);
        grpcPublishOptions.setPort(publishPort);

        UFilterPickerPickBroker::GRPCServerOptions grpcSubscribeOptions;
        grpcSubscribeOptions.setHost(host);
        grpcSubscribeOptions.setPort(subscribePort);

        UFilterPickerPickBroker::PublishServiceOptions publishOptions;
        publishOptions.setGRPCOptions(grpcPublishOptions);

        UFilterPickerPickBroker::SubscribeServiceOptions subscribeOptions;
        subscribeOptions.setGRPCOptions(grpcSubscribeOptions);

        UFilterPickerPickBroker::BrokerOptions options;
        REQUIRE_NOTHROW(options.setPublishServiceOptions(publishOptions));
        REQUIRE_NOTHROW(options.setSubscribeServiceOptions(subscribeOptions));
        REQUIRE_NOTHROW(options.setQueueCapacity(queueCapacity));
        REQUIRE_NOTHROW(options.setPickRetentionInterval(retentionInterval));

        REQUIRE(options.getPublishServiceOptions().getGRPCOptions().getPort() == publishPort);
        REQUIRE(options.getSubscribeServiceOptions().getGRPCOptions().getPort() == subscribePort);
        REQUIRE(options.getQueueCapacity() == queueCapacity);
        REQUIRE(options.getPickRetentionInterval() == retentionInterval);
    }
}

TEST_CASE("UFilterPickerPickBroker", "[PickStoreOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::PickStoreOptions options;
        REQUIRE(options.getMaximumQueueCapacity() == 8192);
        REQUIRE(options.getMaximumRetentionDuration() == std::chrono::minutes {15});
    }
    SECTION("Options")
    {
        constexpr int maxQueueCapacity{732};
        constexpr std::chrono::seconds retention{884};
        UFilterPickerPickBroker::PickStoreOptions options;
        REQUIRE_THROWS(options.setMaximumQueueCapacity(0));
        REQUIRE_NOTHROW(options.setMaximumQueueCapacity(maxQueueCapacity));
        REQUIRE_THROWS(options.setMaximumRetentionDuration(
            std::chrono::seconds {0}));
        REQUIRE_NOTHROW(options.setMaximumRetentionDuration(retention));

        const UFilterPickerPickBroker::PickStoreOptions copy{options};
        REQUIRE(options.getMaximumQueueCapacity() == maxQueueCapacity);
        REQUIRE(options.getMaximumRetentionDuration() == retention);
    }
}
*/
