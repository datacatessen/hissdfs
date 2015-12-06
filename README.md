# HissDFS

A simple implementation of GFS/HDFS using Python.  Intended to demonstrate basic principles and be awesome.  Not intended for production use, but that would be cool.

Code beautification is TBD

### Feature List
* Single NameServer
    * Maintains file -> block -> DataServers mapping
    * Receives Heartbeats and Block Reports from DataServers
    * Returns file metadata to clients about where they can find their files
    * Invalidates blocks based on DataServer heartbeat timeouts or if a DataServer unregisters itself
    * Auto-replicates under-replicated blocks to other DataServers in the cluster
* Multiple DataServers
    * Stores blocks in a single storage directory along with ID
    * Sends Heartbeats and Block Reports to NameServer
    * Manages client I/O for reading and writing blocks of data
        * Currently, one file = one block
    * Registers/unregisters with NameServer on startup/shutdown
    * Includes all blocks in storage directory in initial Block Report (restart from failure scenario)