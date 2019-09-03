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

notes:

1. This only supports copying from the client node to the server node at the moment.
It would be fairly straightforward to enhance it to allow copies in the other direction
as well. Let me know if you would like to have that functionality.

2. This currently requires the destination be a filename. Thus, one cannot give just the 
directory on the destination. Relative filenames will be relative to the directory the
server was started in.  Use the "-P" option to create the destination directory if it
doesn't already exist.

3. This was written to run on some memory-heavy machines so the default buffer sizes
are quite large. The may be changed with some command-line options (see below).

## Building

Commands for downloading and building are below. There is a SConscript file which can be
used if you have scons installed. Since the source all gets compile into a single 
program though, it is also easy to just build it via a single command as shown.

> git clone https://github.com/JeffersonLab/hdrdmacp

> cd hdrdmacp

> c++ -o hdrdmacp *.cc -libverbs -lz

## Running

Run the program with "--help" to get the help statement:

<pre>

Hall-D RDMA file copy server/client

Usage:

   hdrdmacp [options] srcfile host:[port:]destfile
   hdrdmacp -s

This program can be used as both the server and client to copy a
file from the local host to a remote host using RDMA over IB.
This currently does not support copying files from the remote
server back to the client. It also only supports copying a single
file per connection at the moment. In server mode it can accept
multiple simultaneous connections and so can receive any number
of files. In client mode however, only a single file can be
tranferred. Run multiple clients to transfer multiple files.

Note: In the options below: 
    CMO=Client Mode Only
    SMO=Server Mode Only

 options:
    -c         calculate checksum (adler32 currently only prints) (CMO)
    -d         delete source file upon successful transfer (CMO)
    -h         print this usage statement.
    -m  GB     total memory to allocate (def. 8GB for server, 1GB for client)
    -n  Nbuffs number of buffers to break the allocated memory into. This
               will determine the size of RDMA transfer requests.
    -P         make parent directory path on remote host if needed (CMO)
    -p port    set remote port to connect to (can also be given in dest name) (CMO)
    -s         server mode (SMO)
    -sp        server port to listen on (default is 10470) (SMO)

NOTES:
  1. The full filename on the destination must be specfied, not just
     a directory. This is not checked for automatically so the user
     must take care.

  2. The remote host and port refer to a TCP connection that is
     first made to exchange the RDMA connection info. The file is
     then transferred via RDMA.

  3. The destination port may be speficied either via the -p option
     or as part of the destination argument. e.g.
        my.remote.host:12345:/path/to/my/destfilename
     if both are given then the one given in the destination argument
     is used.

  4. If you see an error about "Unable to register memory region!" then
     this may be due to the maximum locked memory size. Check this by
     running "limit" if using tcsh and looking for "memorylocked". If
     using bash, then run "ulimit -a" and look for "max locked memory".
     These should be set to "unlimited". On some of our systems this defaults
     to 64kB and would not honor global settings. A wierd work around was to
     do a "su $USER" which set it to "unlimited". (I do not understand why.)

Example:
  On destination host run:  hdrdmacp -s

  On source host run:  hdrdmacp /path/to/my/srcfile my.remote.host:/path/to/my/destfile

Note that the above will fail if /path/to/my does not already exist on
my.remote.host. If you add the -P argument then /path/to/my will be 
automatically create (if it doesn't already exist).
</pre>




