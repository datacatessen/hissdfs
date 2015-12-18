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


### Usage

Clone the repository, change to the repo dir, edit the config files to your liking, making sure the hostname is set correctly.  You can run multiple dataservers on the same node, as long as the data directories and ports are separate.

```bash
> cd hissdfs/
> hostname -f
MacBook.home 
> vi configs/server1.json #And all the other ones while you're at it
```

Now, start the name server and data servers in separate terminals

```bash
python hissdfs.py configs/server1.json nameserver
python hissdfs.py configs/server1.json dataserver 41414
python hissdfs.py configs/server2.json dataserver 42424
python hissdfs.py configs/server3.json dataserver 43434
```

After the services have started, you can use the client shell to communicate with the file system

```bash
> python Client.py configs/server1.json ls
> python Client.py configs/server1.json put README.md datafile
> python Client.py configs/server1.json ls
datafile
> python Client.py configs/server1.json cat datafile
# HissDFS

A simple implementation of GFS/HDFS using Python.  Intended to demonstrate basic principles and be awesome.  Not intended for production use, but that would be cool.

....
```

