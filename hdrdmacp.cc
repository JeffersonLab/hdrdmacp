
#ifdef __GNUC__
#include <unistd.h>
#endif

#include "IhdRDMA.h"

#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
using namespace std;
using std::chrono::steady_clock;
using std::chrono::duration;
using std::chrono::duration_cast;

bool HDRDMA_IS_SERVER = false;
bool HDRDMA_IS_CLIENT = false;

int HDRDMA_LOCAL_PORT  = 10470;
int HDRDMA_REMOTE_PORT = 10470;
std::string HDRDMA_REMOTE_ADDR = "gluon47.jlab.org";
int HDRDMA_CONNECTION_TIMEOUT = 10; // seconds
string HDRDMA_SRCFILENAME = "";
std::string HDRDMA_DSTFILENAME = "";
bool HDRDMA_DELETE_AFTER_SEND  = false;
bool HDRDMA_CALCULATE_CHECKSUM = false;
bool HDRDMA_MAKE_PARENT_DIRS   = false;
uint64_t HDRDMA_BUFF_LEN_GB = 0;       // defaults differ for server and client modes
uint64_t HDRDMA_NUM_BUFF_SECTIONS = 0; // so these are set in ParseCommandLineArguments

atomic<uint64_t> BYTES_RECEIVED_TOT(0);

void ParseCommandLineArguments( int narg, char *argv[] );
void Usage(void);


//-------------------------------------------------------------
// main
//-------------------------------------------------------------
int main(int narg, char *argv[])
{
	ParseCommandLineArguments( narg, argv );

	// Create an hdRDMA object
	const hdrdma::config hdrdma_config(HDRDMA_BUFF_LEN_GB * 1000'000'000 / HDRDMA_NUM_BUFF_SECTIONS, HDRDMA_NUM_BUFF_SECTIONS);
	auto hdrdma = hdrdma::Create(hdrdma_config);

	// Listen for remote peers if we are in server mode
	// This will launch a thread and listen for any remote connections.
	// Any that are made will have their RDMA transfer information setup
	// and stored in the hdRDMA object and be made available for transfers.
	// This will return right away so one must check the GetNpeers() method
	// to see when a connection is made.
	if( HDRDMA_IS_SERVER ){
		hdrdma->Listen( HDRDMA_LOCAL_PORT );

		// We want to report 10sec, 1min, and 5min averages
		auto t_last_10sec = steady_clock::now();
		auto t_last_1min = t_last_10sec;
		auto t_last_5min = t_last_10sec;
		uint64_t last_bytes_received_10sec = BYTES_RECEIVED_TOT;
		uint64_t last_bytes_received_1min  = BYTES_RECEIVED_TOT;
		uint64_t last_bytes_received_5min  = BYTES_RECEIVED_TOT;

		while( true ){
			
			hdrdma->Poll();
			
			auto now = steady_clock::now();
			auto duration_10sec = duration_cast<std::chrono::seconds>(now - t_last_10sec);
			auto duration_1min  = duration_cast<std::chrono::seconds>(now - t_last_1min);
			auto duration_5min  = duration_cast<std::chrono::seconds>(now - t_last_5min);
			auto delta_t_10sec  = duration_10sec.count();
			auto delta_t_1min   = duration_1min.count();
			auto delta_t_5min   = duration_5min.count();

			if( delta_t_10sec >= 10.0 ){
				double GB_received = (BYTES_RECEIVED_TOT - last_bytes_received_10sec)/1000000000;
				double rate = GB_received/delta_t_10sec;
				cout << "=== [10 sec avg.] " << rate << " GB/s  --  " << (double)BYTES_RECEIVED_TOT/1.0E12 << " TB total received" << endl;
				t_last_10sec = now;
				last_bytes_received_10sec = BYTES_RECEIVED_TOT;
			}

			if( delta_t_1min >= 60.0 ){
				double GB_received = (BYTES_RECEIVED_TOT - last_bytes_received_1min)/1000000000;
				double rate = GB_received/delta_t_1min;
				cout << "=== [ 1 min avg.] " << rate << " GB/s" << endl;
				t_last_1min = now;
				last_bytes_received_1min = BYTES_RECEIVED_TOT;
			}

			if( delta_t_5min >= 300.0 ){
				double GB_received = (BYTES_RECEIVED_TOT - last_bytes_received_5min)/1000000000;
				double rate = GB_received/delta_t_5min;
				cout << "=== [ 5 min avg.] " << rate << " GB/s" << endl;
				t_last_5min = now;
				last_bytes_received_5min = BYTES_RECEIVED_TOT;
			}

			std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );
		}

		// Stop server from listening (if one is)
		hdrdma->StopListening();
	}	

	// Connect to remote peer if we are in client mode.
	// This will attempt to connect to an hdRDMA object listening on the
	// specified host/port. If a connection is made then the RDMA transfer
	// information will be exchanged and stored in the hdRDMA object and
	// be made available for transfers. If the connection cannot be made 
	// then it will exit the program with an error message.
	if( HDRDMA_IS_CLIENT ){
		hdrdma->Connect( HDRDMA_REMOTE_ADDR, HDRDMA_REMOTE_PORT );
		hdrdma->SendFile( HDRDMA_SRCFILENAME, HDRDMA_DSTFILENAME, HDRDMA_DELETE_AFTER_SEND, HDRDMA_CALCULATE_CHECKSUM, HDRDMA_MAKE_PARENT_DIRS );
	}
	
	return 0;
}

//-------------------------------------------------------------
// ParseCommandLineArguments
//-------------------------------------------------------------
void ParseCommandLineArguments( int narg, char *argv[] )
{
	std::vector<std::string> vfnames;

	for( int i=1; i<narg; i++){
		string arg( argv[i] );
		string next = (i+1)<narg ? argv[i+1]:"";
		
		if( arg=="-h" || arg=="--help" ){
			Usage();
			exit(0);
		}else if( arg=="-s" || arg=="--server" ){
			HDRDMA_IS_SERVER = true;
		}else if( arg=="-p"){
			HDRDMA_REMOTE_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-sp"){
			HDRDMA_LOCAL_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-d"){
			HDRDMA_DELETE_AFTER_SEND = true;
		}else if( arg=="-c"){
			HDRDMA_CALCULATE_CHECKSUM = true;
		}else if( arg=="-P"){
			HDRDMA_MAKE_PARENT_DIRS = true;
		}else if( arg=="-m"){
			HDRDMA_BUFF_LEN_GB = atoi( next.c_str() );
			i++;
		}else if( arg=="-n"){
			HDRDMA_NUM_BUFF_SECTIONS = atoi( next.c_str() );
			i++;
		}else if( arg[0]!='-'){
			vfnames.push_back( arg.c_str() );
			HDRDMA_IS_CLIENT = true;
		}
	}
	
	if( !HDRDMA_IS_SERVER ){
		// We are the client (i.e. we are sending the file)
		if( vfnames.size() != 2 ){
			cout << "ERROR: exactly 2 arguments needed: src dst" << endl;
			Usage();
			exit( -50 );
		}
		
		HDRDMA_SRCFILENAME = vfnames[0];
		
		// Extract destination host, port, and filename from dest
		auto pos = vfnames[1].find(":");
		if( pos == string::npos ){
			cout << "ERROR: destination must be in the form host:[port:]dest_filename (you entered " << vfnames[1] << ")" << endl;
			exit(-51);
		}
		HDRDMA_REMOTE_ADDR = vfnames[1].substr(0, pos);
		
		auto pos2 = vfnames[1].find(":", pos+1);
		if( pos2 == string::npos ){
			// Set memory region size for client mode. Use default if user didn't specify
			if( HDRDMA_BUFF_LEN_GB == 0 ) HDRDMA_BUFF_LEN_GB = 1;
			if( HDRDMA_NUM_BUFF_SECTIONS == 0 ) HDRDMA_NUM_BUFF_SECTIONS = 4;

			// port not specfied in dest string
			HDRDMA_DSTFILENAME = vfnames[1].substr(pos+1);
		}else{
			// Set memory region size for server mode. Use default if user didn't specify
			if( HDRDMA_BUFF_LEN_GB == 0 ) HDRDMA_BUFF_LEN_GB = 8;
			if( HDRDMA_NUM_BUFF_SECTIONS == 0 ) HDRDMA_NUM_BUFF_SECTIONS = 32;
		
			// port is specfied in dest string
			auto portstr = vfnames[1].substr(pos+1, pos2-pos-1);
			HDRDMA_REMOTE_PORT = atoi( portstr.c_str() );
			HDRDMA_DSTFILENAME = vfnames[1].substr(pos2+1);
		}
	}
}

//-------------------------------------------------------------
// Usage
//-------------------------------------------------------------
void Usage(void)
{
	cout << endl;
	cout << "Hall-D RDMA file copy server/client" <<endl;
	cout << endl;
	cout << "Usage:" <<endl;
	cout << endl;
	cout << "   hdrdmacp [options] srcfile host:[port:]destfile" << endl;
	cout << "   hdrdmacp -s" << endl;
	cout << endl;
	cout << "This program can be used as both the server and client to copy a" << endl;
	cout << "file from the local host to a remote host using RDMA over IB." << endl;
	cout << "This currently does not support copying files from the remote" << endl;
	cout << "server back to the client. It also only supports copying a single" << endl;
	cout << "file per connection at the moment. In server mode it can accept" << endl;
	cout << "multiple simultaneous connections and so can receive any number" << endl;
	cout << "of files. In client mode however, only a single file can be" << endl;
	cout << "tranferred. Run multiple clients to transfer multiple files." << endl;
	cout << endl;
	cout << "Note: In the options below: " << endl;
	cout << "    CMO=Client Mode Only" << endl;
	cout << "    SMO=Server Mode Only" << endl;
	cout << endl;
	cout << " options:" <<endl;
	cout << "    -c         calculate checksum (adler32 currently only prints) (CMO)" << endl;
	cout << "    -d         delete source file upon successful transfer (CMO)" << endl;
	cout << "    -h         print this usage statement." << endl;
	cout << "    -m  GB     total memory to allocate (def. 8GB for server, 1GB for client)" << endl;
	cout << "    -n  Nbuffs number of buffers to break the allocated memory into. This" << endl;
	cout << "               will determine the size of RDMA transfer requests." << endl;
	cout << "    -P         make parent directory path on remote host if needed (CMO)" << endl;
	cout << "    -p port    set remote port to connect to (can also be given in dest name) (CMO)" << endl;
	cout << "    -s         server mode (SMO)" << endl;
	cout << "    -sp        server port to listen on (default is 10470) (SMO)" << endl;
	cout << endl;
	cout << "NOTES:" << endl;
	cout << "  1. The full filename on the destination must be specfied, not just" << endl;
	cout << "     a directory. This is not checked for automatically so the user" <<endl;
	cout << "     must take care." << endl;
	cout << endl;
	cout << "  2. The remote host and port refer to a TCP connection that is" << endl;
	cout << "     first made to exchange the RDMA connection info. The file is" << endl;
	cout << "     then transferred via RDMA." << endl;
	cout << endl;
	cout << "  3. The destination port may be speficied either via the -p option" << endl;
	cout << "     or as part of the destination argument. e.g." << endl;
	cout << "        my.remote.host:12345:/path/to/my/destfilename" << endl;
	cout << "     if both are given then the one given in the destination argument" << endl;
	cout << "     is used." << endl;
	cout << endl;
	cout << "  4. If you see an error about \"Unable to register memory region!\" then" << endl;
	cout << "     this may be due to the maximum locked memory size. Check this by" <<endl;
	cout << "     running \"limit\" if using tcsh and looking for \"memorylocked\". If" << endl;
	cout << "     using bash, then run \"ulimit -a\" and look for \"max locked memory\"." << endl;
	cout << "     These should be set to \"unlimited\". On some of our systems this defaults" << endl;
	cout << "     to 64kB and would not honor global settings. A wierd work around was to" << endl;
	cout << "     do a \"su $USER\" which set it to \"unlimited\". (I do not understand why.)" << endl;
	cout << endl;
	cout << "Example:" << endl;
	cout << "  On destination host run:  hdrdmacp -s" << endl;
	cout << endl;
	cout << "  On source host run:  hdrdmacp /path/to/my/srcfile my.remote.host:/path/to/my/destfile" << endl;
	cout << endl;
	cout << "Note that the above will fail if /path/to/my does not already exist on" << endl;
	cout << "my.remote.host. If you add the -P argument then /path/to/my will be " << endl;
	cout << "automatically create (if it doesn't already exist)." << endl;
	cout << endl;
}
