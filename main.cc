
#include <unistd.h>

//#include <infiniband/verbs.h>
#include <hdRDMA.h>

#include <iostream>
#include <vector>
using namespace std;

bool HDRDMA_IS_SERVER = false;
bool HDRDMA_IS_CLIENT = false;

int HDRDMA_LOCAL_PORT  = 12345;
int HDRDMA_REMOTE_PORT = 12345;
std::string HDRDMA_REMOTE_ADDR = "gluon47.jlab.org";
int HDRDMA_CONNECTION_TIMEOUT = 10; // seconds
string HDRDMA_SRCFILENAME = "";
std::string HDRDMA_DSTFILENAME = "";

void ParseCommandLineArguments( int narg, char *argv[] );


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
			
			hdrdma.PollCQ();

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
		hdrdma.SendFile( HDRDMA_SRCFILENAME, HDRDMA_DSTFILENAME );
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
		
		if( arg=="-s" || arg=="--server" ){
			HDRDMA_IS_SERVER = true;
		}else if( arg=="-p"){
			HDRDMA_REMOTE_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-sp"){
			HDRDMA_LOCAL_PORT = atoi( next.c_str() );
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
			exit( -50 );
		}
		
		HDRDMA_SRCFILENAME = vfnames[0];
		auto pos = vfnames[1].find(":");
		if( pos == string::npos ){
			cout << "ERROR: destination must be in the form host:dest_filename (you entered " << vfnames[1] << ")" << endl;
			exit(-51);
		}
		HDRDMA_DSTFILENAME = vfnames[1].substr(pos+1);
		HDRDMA_REMOTE_ADDR = vfnames[1].substr(0, pos);
	}
}


