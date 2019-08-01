
#include <unistd.h>

//#include <infiniband/verbs.h>
#include <hdRDMA.h>

#include <iostream>
#include <vector>
using namespace std;

bool HDRDMA_IS_SERVER = false;
bool HDRDMA_IS_CLIENT = false;

int HDRDMA_LOCAL_PORT  = 10470;
int HDRDMA_REMOTE_PORT = 10470;
std::string HDRDMA_REMOTE_ADDR = "gluon47.jlab.org";
int HDRDMA_CONNECTION_TIMEOUT = 10; // seconds
string HDRDMA_SRCFILENAME = "";
std::string HDRDMA_DSTFILENAME = "";
bool HDRDMA_DELETE_AFTER_SEND = false;
bool HDRDMA_CALCULATE_CHECKSUM = false;

void ParseCommandLineArguments( int narg, char *argv[] );
void Usage(void);


//-------------------------------------------------------------
// main
//-------------------------------------------------------------
int main(int narg, char *argv[])
{
	ParseCommandLineArguments( narg, argv );

	// Create an hdRDMA object
	hdRDMA hdrdma;

	// Listen for remote peers if we are in server mode
	// This will launch a thread and listen for any remote connections.
	// Any that are made will have their RDMA transfer information setup
	// and stored in the hdRDMA object and be made available for transfers.
	// This will return right away so one must check the GetNpeers() method
	// to see when a connection is made.
	if( HDRDMA_IS_SERVER ){
		hdrdma.Listen( HDRDMA_LOCAL_PORT );

		while( true ){
			
			hdrdma.Poll();
			
			std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );
		}

		// Stop server from listening (if one is)
		hdrdma.StopListening();
	}	

	// Connect to remote peer if we are in client mode.
	// This will attempt to connect to an hdRDMA object listening on the
	// specified host/port. If a connection is made then the RDMA transfer
	// information will be exchanged and stored in the hdRDMA object and
	// be made available for transfers. If the connection cannot be made 
	// then it will exit the program with an error message.
	if( HDRDMA_IS_CLIENT ){
		hdrdma.Connect( HDRDMA_REMOTE_ADDR, HDRDMA_REMOTE_PORT );
		hdrdma.SendFile( HDRDMA_SRCFILENAME, HDRDMA_DSTFILENAME, HDRDMA_DELETE_AFTER_SEND, HDRDMA_CALCULATE_CHECKSUM );
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
			// port not specfied in dest string
			HDRDMA_DSTFILENAME = vfnames[1].substr(pos+1);
		}else{
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
	cout << "Note: all of the options below are for client mode except for \"-s\"" << endl;
	cout << "and \"-sp\"." << endl;
	cout << endl;
	cout << " options:" <<endl;
	cout << "    -c         calculate checksum (adler32 currently only prints)" << endl;
	cout << "    -d         delete source file upon successful transfer." << endl;
	cout << "    -h         print this usage statement." << endl;
	cout << "    -p port    set remote port to connect to (can also be given in dest name)" << endl;
	cout << "    -s         server mode" << endl;
	cout << "    -sp        server port to listen on (default is 10470)" << endl;
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
}
