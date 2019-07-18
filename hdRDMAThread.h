

// Every remote peer connection (client or server) is represented
// by a separate thread of execution. Each thread has its own
// hdRDMAThread object which is used to handle communications
// over that connection.

#include <chrono>
#include <vector>
#include <cstdint>
#include <fstream>
#include <string>
#include <thread>
#include <mutex>
#include <map>
#include <exception>

#include <infiniband/verbs.h>

class hdRDMA;

class hdRDMAThread{

	public:	

		// Header info sent as first bytes of data packed
		struct HeaderInfo {
			uint32_t header_len;
			uint16_t buff_type;
			uint16_t flags; // bit 0=first, 1=last
			uint32_t payload;
		}__attribute__ ((packed));

		// Hold info of queue pair on one side of connection
		struct QPInfo {
			uint16_t lid;
			uint32_t qp_num;
		}__attribute__ ((packed));
		
		class Exception:public std::exception{
			public:
				Exception(std::string s):mess(s){}
				const char* what(void) const noexcept { return mess.c_str(); }
				std::string mess;
		};
		
		typedef std::tuple<uint8_t*, uint32_t> bufferinfo;

	
		hdRDMAThread(hdRDMA *hdrdma);
		~hdRDMAThread();
		
		void ThreadRun(int sockfd);
		void PostWR( int id ); // id= index to buffers
		void ExchangeQPInfo( int sockfd );
		void CreateQP(void);
		int SetToRTS(void);
		void ReceiveBuffer(uint8_t *buff, uint32_t buff_len);
		void ClientConnect( int sockfd );
		void SendFile(std::string srcfilename, std::string dstfilename, bool delete_after_send=false, bool calculate_checksum=false);
		void PollCQ(void);
		
		bool stop    = false; // Flag so thread can be told to stop
		bool stopped = false; // Flag so thread can declare it has stopped
		hdRDMA *hdrdma = nullptr;

		struct ibv_comp_channel *comp_channel = nullptr;
		struct ibv_cq *cq = nullptr;
		struct ibv_qp *qp = nullptr;
		struct ibv_recv_wr *bad_wr=nullptr;
		
		// Buffers obtained from hdrdma point to MR
		std::vector< bufferinfo > buffers; // std::get<0>=buff  std::get<1>=buff_len

		QPInfo qpinfo;
		QPInfo remote_qpinfo;
		std::ofstream *ofs = nullptr;
		std::string ofilename;
		uint64_t ofilesize = 0;
		uint32_t crcsum;
		bool calculate_checksum = false;
		
		std::chrono::high_resolution_clock::time_point t1;
		std::chrono::high_resolution_clock::time_point t_last;
		double delta_t_io = 0.0;
		uint64_t Ntransferred = 0; // accumulates for file excluding first buffer
};
