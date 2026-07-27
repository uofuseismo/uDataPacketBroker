#include <string>
#include "uDataPacketBroker/version.hpp"

using namespace UDataPacketBroker;

int Version::getMajor() noexcept
{
    return uDataPacketBroker_MAJOR;
}

int Version::getMinor() noexcept
{
    return uDataPacketBroker_MINOR;
}

int Version::getPatch() noexcept
{
    return uDataPacketBroker_PATCH;
}

//NOLINTBEGIN(bugprone-easily-swappable-parameters)
bool Version::isAtLeast(const int major, const int minor,
                        const int patch) noexcept
//NOLINTEND(bugprone-easily-swappable-parameters)
{
    if (uDataPacketBroker_MAJOR < major){return false;}
    if (uDataPacketBroker_MAJOR > major){return true;}
    if (uDataPacketBroker_MINOR < minor){return false;}
    if (uDataPacketBroker_MINOR > minor){return true;}
    if (uDataPacketBroker_PATCH < patch){return false;}
    return true;
}

std::string Version::getVersion() noexcept
{
    std::string version{uDataPacketBroker_VERSION};
    return version;
}

std::string Version::getTag() noexcept
{
    std::string tag{uDataPacketBroker_GITTAG};
    return tag;
}

std::string Version::getVersionWithTag() noexcept
{
    auto tag = Version::getTag();
    if (tag.empty())
    {
        return Version::getVersion();
    }
    else
    {
        return Version::getVersion() + "-" + tag;
    }
}
