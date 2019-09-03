# hdrdmacp : RDMA client/server for file transfers

## Introduction

The hdrdmacp utlity is a small program that can be used to copy large files between
two computers connected via IB (infiniband) using RDMA (Remote Direct Memory Access).
The program was written to transfer large data sets in the form of many 20GB files
as part of the Data Acquisition for a Nulear Physics experiment at Jefferson Lab (http://www.jlab.org).
To use it, just run one instance in server mode on the machine you want the file transferred TO:

> hdrdmacp -s

And run it in client mode on the machine you wish to transfer the file FROM, giving
it the local file as the first argument and then the remote host+destination directory
as the second:

> hdrdmacp file.dat my.server.host:/path/to/dest/dir

```notes:```
1. This only supports copying from the client node to the server node at the moment.
It would be fairly straightforward to enhance it to allow copies in the other direction
as well. Let me know if you would like to have that functionality.

2. This currently requires the destination be a directory. Thus, one cannot give the 
file different name at the destination than it is on the host. 

3. This was written to run on some memory-heavy machines so the default buffer sizes
are quite large. The may be changed with some command-line options (see below).

## Building


## Running


