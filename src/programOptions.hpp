#ifndef PROGRAM_OPTIONS_HPP
#define PROGRAM_OPTIONS_HPP
#include "uDataPacketBroker/grpcServerOptions.hpp"
#include "uDataPacketBroker/publishServiceOptions.hpp"
#include "uDataPacketBroker/subscribeServiceOptions.hpp"
#include "otelOptions.hpp"
namespace 
{
struct ProgramOptions
{
    /// After this size we have to pop elements from the queue.
    int64_t maximumImportQueueSize{8192};
};
}
#endif
