

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

#ifdef __GNUC__
#define PACK( __Declaration__ ) __Declaration__ __attribute__((__packed__))
#endif

#ifdef _MSC_VER
#define PACK( __Declaration__ ) __pragma( pack(push, 1) ) __Declaration__ __pragma( pack(pop))
#endif

#ifdef __GNUC__
typedef int SOCKET;
#define closesocket close
#define INVALID_SOCKET -1
#endif

class hdRDMAThread{

	public:
	
		enum{
			HI_FIRST_BUFFER       = 0x01,
			HI_LAST_BUFFER        = 0x02,
			HI_LAST_FILE          = 0x04,
			HI_CALCULATE_CHECKSUM = 0x08,
			HI_MAKE_PARENT_DIRS   = 0x10
		}HeaderInfoFlag_t;

		// Header info sent as first bytes of data packed
		PACK(struct HeaderInfo {
			uint32_t header_len;
			uint16_t buff_type;
			uint16_t flags; // bit 0=first, 1=last
			uint32_t payload;
		});

		// Hold info of queue pair on one side of connection
		PACK(struct QPInfo {
			uint16_t lid;
			uint32_t qp_num;
		});
		
		class Exception:public std::exception{
			public:
				Exception(std::string s):mess(s){}
				const char* what(void) const noexcept { return mess.c_str(); }
				std::string mess;
		};

		struct bufferinfo
		{
			bufferinfo(uint8_t* buff, uint32_t buff_len, struct ibv_mr* mr) : Buffer(buff), BufferLen(buff_len), MR(mr) {}
			uint8_t* Buffer = nullptr;
			uint32_t BufferLen = 0;
			struct ibv_mr* MR = nullptr;
		};
		
	
		hdRDMAThread(hdRDMA *hdrdma);
		~hdRDMAThread();
		
		void ThreadRun(SOCKET sockfd);
		void TryThreadRun(SOCKET sockfd);
		void PostWR( int id ); // id= index to buffers
		void ExchangeQPInfo( SOCKET sockfd );
		void CreateQP(void);
		int SetToRTS(void);
		void ReceiveBuffer(uint8_t *buff, uint32_t buff_len);
		void ClientConnect( SOCKET sockfd );
		void SendFile(std::string srcfilename, std::string dstfilename, bool delete_after_send=false, bool calculate_checksum=false, bool makeparentdirs=false);
		void TrySendFile(std::string srcfilename, std::string dstfilename, bool delete_after_send=false, bool calculate_checksum=false, bool makeparentdirs=false);
		void PollCQ(void);
		bool makePath( const std::string &path );

		void Dispose();
		
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
		std::unique_ptr<std::ofstream> ofs;
		std::string ofilename;
		uint64_t ofilesize = 0;
		uint32_t crcsum;
		bool calculate_checksum = false;
		
		std::chrono::high_resolution_clock::time_point t1;
		std::chrono::high_resolution_clock::time_point t_last;
		double delta_t_io = 0.0;
		uint64_t Ntransferred = 0; // accumulates for file excluding first buffer
};
