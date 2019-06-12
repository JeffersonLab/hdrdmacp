
#include <hdRDMA.h>

#include <unistd.h>

#include <iostream>
#include <atomic>
#include <chrono>
#include <fstream>

using std::cout;
using std::cerr;
using std::endl;
using namespace std::chrono;

//-------------------------------------------------------------
// hdRDMA
//
// hdRDMA constructor. This will look for IB devices and set up
// for RDMA communications on the first one it finds.
//-------------------------------------------------------------
hdRDMA::hdRDMA()
{
	cout << "Looking for IB devices ..." << endl;
	int num_devices = 0;
	struct ibv_device **devs = ibv_get_device_list( &num_devices );
	
	// List devices
	cout << endl << "=============================================" << endl;
	cout << "Found " << num_devices << " devices" << endl;
	cout << "---------------------------------------------" << endl;
	for(int i=0; i<num_devices; i++){
	
		const char *transport_type = "unknown";
		switch( devs[i]->transport_type ){
			case IBV_TRANSPORT_IB:
				transport_type = "IB";
				break;
			case IBV_TRANSPORT_IWARP:
				transport_type = "IWARP";
				break;
			case IBV_EXP_TRANSPORT_SCIF:
				transport_type = "SCIF";
				break;
		}
		
		// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		// Here we want to check the lid of each device but to do so we 
		// must open the device and get the port attributes. We need to
		// do this to determine which device is actually connected to the
		// IB network since only connected ones will have lid!=0.
		// We remember the last device in the list with a non-zero lid
		// and use that.
		uint64_t lid = 0;

		// Open device
		ctx = ibv_open_device(devs[i]);
		if( ctx ){
			uint8_t port_num = 1;
			struct ibv_port_attr my_port_attr;
			ibv_query_port( ctx, port_num, &my_port_attr);
			lid = my_port_attr.lid;
			if( lid != 0) dev = devs[i]; // TODO: allow user to specify something other than first device
			ibv_close_device( ctx );
		}
		// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
		cout << "   device " << i
			<< " : " << devs[i]->name
			<< " : " << devs[i]->dev_name
			<< " : " << transport_type
			<< " : " << ibv_node_type_str(devs[i]->node_type)
			<< " : GUID=" << ibv_get_device_guid(devs[i])
			<< " : lid=" << lid
			<< endl;
	}
	cout << "=============================================" << endl << endl;
	
	// Open device
	ctx = ibv_open_device(dev);
	if( !ctx ){
		cout << "Error opening IB device context!" << endl;
		exit(-11);
	}
	
	// Get device and port attributes
	uint8_t port_num = 1;
	int index = 0;
	ibv_gid gid;
	ibv_query_device( ctx, &attr);
	ibv_query_port( ctx, port_num, &port_attr);
	ibv_query_gid(ctx, port_num, index, &gid);

	cout << "Device " << dev->name << " opened."
		<< " num_comp_vectors=" << ctx->num_comp_vectors
		<< " max_mr_size=" << attr.max_mr_size
		<< endl;

	// Allocate protection domain
	pd = ibv_alloc_pd(ctx);
	if( !pd ){
		cout << "ERROR allocation protection domain!" << endl;
		exit(-12);
	}
	
	// Allocate a 4GB buffer and create a memory region pointing to it.
	// We will split this one memory region among multiple receive requests
	// n.b. initial tests failed on transfer for buffers larger than 1GB
	// so the buff_section_len is set to 1GB
	buff_section_len = 1000000000;
	num_buff_sections = 4;
	buff_len = num_buff_sections*buff_section_len;
	buff = new uint8_t[buff_len];
	if( !buff ){
		cout << "ERROR: Unable to allocate buffer!" << endl;
		exit(-13);
	}
	errno = 0;
	auto access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
	mr = ibv_reg_mr( pd, buff, buff_len, access);
	if( !mr ){
		cout << "ERROR: Unable to register memory region! errno=" << errno << endl;
		exit( -14 );
	}
	
	// Create completion channel and completion queue
	int cq_size = 8; // how much should this be?
	comp_channel = ibv_create_comp_channel( ctx );
	cq = ibv_create_cq( ctx, cq_size, NULL, comp_channel, 0);
	if( !cq ){
		cout << "ERROR: Unable to create Completion Queue! errno=" << errno << endl;
		exit(-15);
	}
	
	// Create Queue Pair
	ibv_qp_init_attr qp_init_attr;
	bzero( &qp_init_attr, sizeof(qp_init_attr) );
	qp_init_attr.send_cq = cq;
	qp_init_attr.recv_cq = cq;
	qp_init_attr.cap.max_send_wr  = 1;
	qp_init_attr.cap.max_recv_wr  = 4;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp = ibv_create_qp( pd, &qp_init_attr );
	if( !qp ){
		cout << "ERROR: Unable to create QP! errno=" << errno << endl;
		exit(-16);
	}
	
	// Set our QP info so it can be sent to remote hosts when
	// socket connection is established.
	qpinfo.lid = port_attr.lid;
	qpinfo.qp_num = qp->qp_num;
	
	cout << "local    lid: " << qpinfo.lid << endl;
	cout << "local qp_num: " << qpinfo.qp_num << endl;
	
	// Post receive work requests
	// This sets us up to receive data in our QP and must be done before
	// the send request is posted. We split the buffer associated with
	// the mr up into smaller sections and set up a receive request for
	// each section. As each receive request is used, another is posted in
	// its place.
	//
	// n.b. this is done whether we are server or client and is using
	// our one MR. This really only needs to be set up on the side
	// that is receiving data, but we do it in both caes for now.
	for(int i=0; i<num_buff_sections; i++ ){
		struct ibv_recv_wr wr;
		struct ibv_sge sge;
		bzero( &wr, sizeof(wr));
		bzero( &sge, sizeof(sge));
		wr.wr_id = i;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		sge.addr = (uintptr_t)&buff[i*buff_section_len];
		sge.length = buff_section_len;
		sge.lkey = mr->lkey;
		auto ret = ibv_post_recv( qp, &wr, &bad_wr);
		if( ret != 0 ){
			cout << "ERROR: ibv_post_recv returned non zero value (" << ret << ")" << endl;
		}
	}
	
}

//-------------------------------------------------------------
// ~hdRDMA
//-------------------------------------------------------------
hdRDMA::~hdRDMA()
{
	// Close and free everything
	if(           qp!=nullptr ) ibv_destroy_qp( qp );
	if(           cq!=nullptr ) ibv_destroy_cq ( cq );
	if( comp_channel!=nullptr ) ibv_destroy_comp_channel( comp_channel );
	if(           mr!=nullptr ) ibv_dereg_mr( mr );
	if(         buff!=nullptr ) delete[] buff;
	if(           pd!=nullptr ) ibv_dealloc_pd( pd );
	if(          ctx!=nullptr ) ibv_close_device( ctx );

	if( server_sockfd ) shutdown( server_sockfd, SHUT_RDWR );
}

//-------------------------------------------------------------
// Listen
//
// Set up server and listen for connections from remote hosts
// wanting to trade RDMA connection information.
//-------------------------------------------------------------
void hdRDMA::Listen(int port)
{
	// Create socket, bind it and put it into the listening state.	
	struct sockaddr_in addr;
	bzero( &addr, sizeof(addr) );
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons( port );
	
	server_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	auto ret = bind( server_sockfd, (struct sockaddr*)&addr, sizeof(addr) );
	if( ret != 0 ){
		cout << "ERROR: binding server socket!" << endl;
		exit(-2);
	}
	listen(server_sockfd, 5);
	
	// Create separate thread to accept socket connections so we don't block
	std::atomic<bool> thread_started(false);
	server_thread = new std::thread([&](){
		
		// Loop forever accepting connections
		cout << "Listening for connections on port ... " << port << endl;
		thread_started = true;
		
		while( !done ){
			int peer_sockfd   = 0;
			struct sockaddr_in peer_addr;
			socklen_t peer_addr_len = sizeof(struct sockaddr_in);
			peer_sockfd = accept(server_sockfd, (struct sockaddr *)&peer_addr, &peer_addr_len);
			if( peer_sockfd > 0 ){
				cout << "Connected!" <<endl;
				cout << "Connection from " << inet_ntoa(peer_addr.sin_addr) << endl;
				SendQPInfo( peer_sockfd );
				ReceiveQPInfo( peer_sockfd ); //n.b. automatically links remote QP and sets RTS
				Npeers++;
			}else{
				cout << "Failed connection!" <<endl;
				break;
			}
		} // !done
	
	}); 
	
	// Wait for thread to start up so it's listening message gets printed
	// before rest of program continues.
	while(!thread_started) std::this_thread::yield();
	
}

//-------------------------------------------------------------
// StopListening
//-------------------------------------------------------------
void hdRDMA::StopListening(void)
{
	if( server_thread ){
		cout << "Waiting for server to finish ..." << endl;
		done = true;
		server_thread->join();
		delete server_thread;
		server_thread = nullptr;
		if( server_sockfd ) shutdown( server_sockfd, SHUT_RDWR );
		server_sockfd = 0;
	}else{
		cout << "Server not running." <<endl;
	}
}

//-------------------------------------------------------------
// Connect
//-------------------------------------------------------------
void hdRDMA::Connect(std::string host, int port)
{
	// Get IP address based on server hostname
	struct addrinfo hints;
	struct addrinfo *result;
	char addrstr[100];
	void *ptr;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_UNSPEC;
	hints.ai_flags |= AI_CANONNAME;

	auto ret = getaddrinfo(host.c_str(), NULL, &hints, &result);
	while( result ){
		inet_ntop( result->ai_family, result->ai_addr->sa_data, addrstr, 100);
		switch( result->ai_family ){
			case AF_INET:
				ptr = &((struct sockaddr_in *)result->ai_addr)->sin_addr;
				break;
			case AF_INET6:
				ptr = &((struct sockaddr_in6 *)result->ai_addr)->sin6_addr;
				break;
		}
		inet_ntop( result->ai_family, ptr, addrstr, 100 );
		
		cout << "IP address: " << addrstr << " (" << result->ai_canonname << ")" << endl;
		
		result = result->ai_next;
	}
	

	// Create socket and connect it to remote host	
	struct sockaddr_in addr;
	bzero( &addr, sizeof(addr) );
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr( addrstr );
	addr.sin_port = htons( port );
	
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	ret = connect( sockfd, (struct sockaddr*)&addr, sizeof(addr) );
	if( ret != 0 ){
		cout << "ERROR: connecting to server: " << host << " (" << inet_ntoa(addr.sin_addr) << ")" << endl;
		exit(-3);
	}else{
		cout << "Connected to " << host << ":" << port << endl;
	}

	// Send my RDMA connection info.
	SendQPInfo( sockfd );
	ReceiveQPInfo( sockfd ); //n.b. automatically links remote QP and sets RTS
}

//-------------------------------------------------------------
// SendQPInfo
//-------------------------------------------------------------
void hdRDMA::SendQPInfo( int sockfd )
{
	int n;
	struct QPInfo tmp_qp_info;

	tmp_qp_info.lid       = htons(qpinfo.lid);
	tmp_qp_info.qp_num    = htonl(qpinfo.qp_num);
    
	n = write(sockfd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
	if( n!= sizeof(struct QPInfo) ){
		cout << "ERROR: Sending QPInfo! Tried sending " << sizeof(struct QPInfo) << " bytes but only " << n << " were sent!" << endl;
		exit(-4);
	}
}

//-------------------------------------------------------------
// ReceiveQPInfo
//
// This will read the QP info from the socket that was sent by the
// remote program. As soon as we receive the info. use it to change
// the QP state to RTS
//-------------------------------------------------------------
void hdRDMA::ReceiveQPInfo( int sockfd )
{
	int n;
	struct QPInfo tmp_qp_info;
    
	n = read(sockfd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
	if( n!= sizeof(struct QPInfo) ){
		cout << "ERROR: Sending QPInfo! Tried sending " << sizeof(struct QPInfo) << " bytes but only " << n << " were sent!" << endl;
		exit(-4);
	}

	remote_qpinfo.lid       = htons(tmp_qp_info.lid);
	remote_qpinfo.qp_num    = htonl(tmp_qp_info.qp_num);
    
	cout << "remote    lid: " << remote_qpinfo.lid << endl;
	cout << "remote qp_num: " << remote_qpinfo.qp_num << endl;
	
	// Set QP state to RTS
	auto ret = SetToRTS();
	if( ret != 0 ) cout << "ERROR: Unable to set QP to RTS state!" << endl;

}

//-------------------------------------------------------------
// SetToRTS
//-------------------------------------------------------------
int hdRDMA::SetToRTS(void)
{
#define IB_MTU		IBV_MTU_4096
#define IB_PORT	1
#define IB_SL		0
	int ret = 0;

	/* change QP state to INIT */
	{
		struct ibv_qp_attr qp_attr;
		bzero( &qp_attr, sizeof(qp_attr) );		
		qp_attr.qp_state        = IBV_QPS_INIT,
		qp_attr.pkey_index      = 0,
		qp_attr.port_num        = IB_PORT,
		qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
	                       IBV_ACCESS_REMOTE_READ |
	                       IBV_ACCESS_REMOTE_ATOMIC |
	                       IBV_ACCESS_REMOTE_WRITE;

		ret = ibv_modify_qp (qp, &qp_attr,
			 IBV_QP_STATE | IBV_QP_PKEY_INDEX |
			 IBV_QP_PORT  | IBV_QP_ACCESS_FLAGS);
		if( ret!=0 ){
			cout << "ERROR: Unable to set QP to INIT state!" << endl;
			return ret;
		}
	}

	/* Change QP state to RTR */
	{
		struct ibv_qp_attr  qp_attr;
		bzero( &qp_attr, sizeof(qp_attr) );		
		qp_attr.qp_state           = IBV_QPS_RTR,
		qp_attr.path_mtu           = IB_MTU,
		qp_attr.dest_qp_num        = remote_qpinfo.qp_num,
		qp_attr.rq_psn             = 0,
		qp_attr.max_dest_rd_atomic = 1,
		qp_attr.min_rnr_timer      = 12,
		qp_attr.ah_attr.is_global  = 0,
		qp_attr.ah_attr.dlid       = remote_qpinfo.lid,
		qp_attr.ah_attr.sl         = IB_SL,
		qp_attr.ah_attr.src_path_bits = 0,
		qp_attr.ah_attr.port_num      = IB_PORT,

		ret = ibv_modify_qp(qp, &qp_attr,
			    IBV_QP_STATE | IBV_QP_AV |
			    IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
			    IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
			    IBV_QP_MIN_RNR_TIMER);
		if( ret!=0 ){
			cout << "ERROR: Unable to set QP to RTR state!" << endl;
			return ret;
		}
	}

	/* Change QP state to RTS */
	{
		struct ibv_qp_attr  qp_attr;
		bzero( &qp_attr, sizeof(qp_attr) );		
		qp_attr.qp_state      = IBV_QPS_RTS,
		qp_attr.timeout       = 14,
		qp_attr.retry_cnt     = 7,
		qp_attr.rnr_retry     = 7,
		qp_attr.sq_psn        = 0,
		qp_attr.max_rd_atomic = 1,

		ret = ibv_modify_qp (qp, &qp_attr,
			     IBV_QP_STATE | IBV_QP_TIMEOUT |
			     IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
			     IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
		if( ret!=0 ){
			cout << "ERROR: Unable to set QP to RTS state!" << endl;
			return ret;
		}
	}

    return ret;
}

//-------------------------------------------------------------
// GetNpeers
//-------------------------------------------------------------
uint32_t hdRDMA::GetNpeers(void)
{
	return Npeers;
}

//-------------------------------------------------------------
// SendFile
//-------------------------------------------------------------
void hdRDMA::SendFile(std::string srcfilename, std::string dstfilename)
{
	// TEMPORARY:
	// Post a send request
	
	// Open file
	std::ifstream ifs(srcfilename.c_str());
	if( !ifs.is_open() ){
		cout <<"ERROR: Unable to open file \"" << srcfilename << "\"!" << endl;
		exit(-40);
	}
	
	// Get filesize
	ifs.seekg(0, ifs.end);
	auto filesize = ifs.tellg();
	ifs.seekg(0, ifs.beg);
	double filesize_GB = (double)filesize*1.0E-9;
	
	cout << "Sending file: " << srcfilename << "->" << dstfilename << "   (" << filesize_GB << " GB)" << endl;
	
	struct ibv_send_wr wr, *bad_wr = nullptr;
	struct ibv_sge sge;
	bzero( &wr, sizeof(wr) );
	bzero( &sge, sizeof(sge) );
	
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED,
	
	sge.lkey = mr->lkey;
	
	// Send buffers
	auto t1 = high_resolution_clock::now();
	uint64_t Ntransferred = 0;
	uint64_t bytes_left = filesize;
	uint32_t Noutstanding_writes = 0;
	double delta_t_io = 0.0;
	for(int i=0; i<1000; i++){ // if sending more than 1000 buffers something is wrong!
		auto id = i%num_buff_sections;
		sge.addr = (uintptr_t)&buff[id*buff_section_len];
		HeaderInfo *hi = (HeaderInfo*)sge.addr;
		hi->buff_type = 1; // buffer holds data for file transfer
		hi->flags = 0x0;
		
		// First buffer must contain destination file name.
		// Subsequent buffers don't.
		if( i==0 ){
			hi->header_len = 256;
			hi->flags |= 0x1; // first buffer of file
			sprintf( (char*)&hi->payload, dstfilename.c_str() );
		}else{
			hi->header_len = sizeof(*hi) - sizeof(hi->payload);
		}
		
		// Calculate bytes to be sent in this buffer
		auto bytes_available = buff_section_len - hi->header_len;
		uint64_t bytes_payload = 0;
		if( bytes_available >= bytes_left ){
			// last buffer of file
			hi->flags |= 0x2;
			bytes_payload = bytes_left;
		}else{
			// intermediate buffer of file
			bytes_payload = bytes_available;
		}
		sge.length = hi->header_len + bytes_payload;

		// Read next block of data directly into mr memory
		auto t_io_start = high_resolution_clock::now();
		ifs.read( &((char*)sge.addr)[hi->header_len], bytes_payload );
		auto t_io_end = high_resolution_clock::now();
		duration<double> duration_io = duration_cast<duration<double>>(t_io_end-t_io_start);
		delta_t_io += duration_io.count();
		
		// Post write
		auto ret = ibv_post_send( qp, &wr, &bad_wr );
		if( ret != 0 ){
			cout << "ERROR: ibv_post_send returned non zero value (" << ret << ")" << endl;
			break;
		}
		Noutstanding_writes++;
		Ntransferred += bytes_payload;
		bytes_left -= bytes_payload;

		// If we've posted data using all available sections of the mr
		// then we need to wait for one to finish so we can recycle it.
		if( Noutstanding_writes>=num_buff_sections ){
			PollCQ();
			Noutstanding_writes--;
		}
		
		if( hi->flags & 0x2 ) break; // this was last buffer of file
	}
	
	// Wait for final buffers to transfer
	while( Noutstanding_writes > 0 ){
		PollCQ();
		Noutstanding_writes--;
	}

	// Calculate total transfer rate and report.
	auto t2 = high_resolution_clock::now();
	duration<double> delta_t = duration_cast<duration<double>>(t2-t1);
	double rate_Gbps = (double)Ntransferred/delta_t.count()*8.0/1.0E9;
	double rate_io_Gbps = (double)Ntransferred/delta_t_io*8.0/1.0E9;
	double rate_ib_Gbps = (double)Ntransferred/(delta_t.count()-delta_t_io)*8.0/1.0E9;
	
	cout << "Transferred " << ((double)Ntransferred*1.0E-9) << " GB in " << delta_t.count() << " sec  (" << rate_Gbps << " Gbps)" << endl;
	cout << "I/O rate reading from file: " << delta_t_io << " sec  (" << rate_io_Gbps << " Gbps)" << endl;
	cout << "IB rate sending file: " << delta_t.count()-delta_t_io << " sec  (" << rate_ib_Gbps << " Gbps)" << endl;

}

//-------------------------------------------------------------
// SendBuffer
//-------------------------------------------------------------
void hdRDMA::SendBuffer(uint8_t *buff, uint32_t buff_len)
{
// 	// Post send request
// 	struct ibv_sge sg_list;
// 	sg_list.addr   = (uintptr_t)buff;
// 	sg_list.length = buff_len;
// 	sg_list.lkey   = mr->lkey;
// 	struct ibv_send_wr wr;
// 	wr.next = NULL;
// 	wr.sg_list = &sg_list;
// 	wr.num_sge = 1;
// 	wr.opcode = IBV_WR_SEND;
// 	struct ibv_send_wr *bad_wr;
// 	if( ibv_post_send(qp, &wr, &bad_wr) ){
// 		cout << "ERROR: Posting send!" << endl;
// 		exit(-14);
// 	}
// 
// 	if( devs!=nullptr ) ibv_free_device_list( devs );
// 
// 	cout << "Done" << endl;

}

//-------------------------------------------------------------
// ReceiveBuffer
//-------------------------------------------------------------
void hdRDMA::ReceiveBuffer(uint8_t *buff, uint32_t buff_len)
{
	HeaderInfo *hi = (HeaderInfo*)buff;
	if( hi->buff_type == 1 ){
		// Buffer holds file information
		if( hi->flags & 0x1 ){
			if( ofs ) {
				cout << "ERROR: Received new file buffer while file " << ofilename << " already open!" << endl;
				ofs->close();
				delete ofs;
			}
			
			ofilename = (char*)&hi->payload;
			cout << "Receiving file: " << ofilename << endl;
			ofs = new std::ofstream( ofilename.c_str() );
			ofilesize = 0;
		}

		if( !ofs ){
			cout << "ERROR: Received file buffer with no file open!" << endl;
			return;
		}
		
		// Write buffer payload to file
		auto data = &buff[hi->header_len];
		auto data_len = buff_len - hi->header_len;
		ofs->write( (const char*)data, data_len );
		ofilesize += data_len;
		
		// If last buffer for file then close it and print stats
		if( hi->flags & 0x2 ){
			if( ofs ){
				ofs->close();
				delete ofs;
				ofs = nullptr;
			}
			Npeers--; // assume remote peer is done sending data
			cout << "Closed file " << ofilename << " with " << ofilesize/1000000 << " MB" << endl;
		}
	}
}

//-------------------------------------------------------------
// PollCQ
//-------------------------------------------------------------
void hdRDMA::PollCQ(void)
{
	cout << "Polling for WR completion ...." << endl;
	
	int num_wc = 1;
	struct ibv_wc wc;
	while( !done ){
		int n = ibv_poll_cq(cq, num_wc, &wc);
		if( n<0 ){
			cout << "ERROR: ibv_poll_cq returned " << n << endl;
			exit(-20);
		}
		if( n == 0 ){
			std::this_thread::sleep_for(std::chrono::microseconds(1));
			continue;
		} 
		
		// Work completed!
		cout << "Work Request completion received!" << endl;
		if( wc.status != IBV_WC_SUCCESS ){
			cout << "ERROR: Status of WC not zero (" << wc.status << ")" << endl;
			return;
		}
		
		switch( wc.opcode ){
			case IBV_WC_SEND: cout << "opcode: IBV_WC_SEND" << endl;
				return; // (allow another wr to be sent)
				break;
			case IBV_WC_RECV:
				cout << "opcode: IBV_WC_RECV (" << wc.byte_len/1000000 << " Mbytes)" << endl;
				ReceiveBuffer( &buff[wc.wr_id*buff_section_len], wc.byte_len);
				break;
			default: cout << "opcode: other" << endl; break;
		}
		
		
		// Re-post the receive request
		auto id = wc.wr_id;
		if( id>= num_buff_sections ){
			cout << "ERROR: Bad id in wc (" << id << ") expected it to be < " << num_buff_sections << endl;
			exit(-30);
		}else if(wc.opcode==IBV_WC_RECV) {
			cout << "re-posting receive request " << id << endl;
			struct ibv_recv_wr wr;
			struct ibv_sge sge;
			bzero( &wr, sizeof(wr));
			bzero( &sge, sizeof(sge));
			wr.wr_id = id;
			wr.sg_list = &sge;
			wr.num_sge = 1;
			sge.addr = (uintptr_t)&buff[id*buff_section_len];
			sge.length = buff_section_len;
			sge.lkey = mr->lkey;
			auto ret = ibv_post_recv( qp, &wr, &bad_wr);
			if( ret != 0 ){
				cout << "ERROR: ibv_post_recv returned non zero value (" << ret << ")" << endl;
			}
		}
	}
}

