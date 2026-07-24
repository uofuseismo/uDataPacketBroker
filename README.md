# About

The uDataPacketBroker receives inbound data packets via gRPC, caches them for a fixed amount of time, and streams
them to interested parties via gRPC.  Moreover, the application is (mostly) resilient to crashes as received packets
are written to disk.  Though there can exist a brief moment before a disk flush action that can result in a few packets
occasionally being lost.  The uDataPacketBrokerIis intended to be run as stateful set in a Kubernetes environment with 
only one instance - i.e., only one application is reading and writing from the underlying RocksDB file.  Communication 
happens via gRPC with the protocol defined [here](https://github.com/uofuseismo/uDataPacketBrokerAPI).  At a high level, 
this application is very similar to Earthscope's [ringserver](https://github.com/EarthScope/ringserver) and I recommend
you use ringserver unless you specifically require application monitoring and/or allowing data writers to exist on different
physical compute nodes.


