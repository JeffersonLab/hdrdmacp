
#include <cstdint>
#include <thread>
#include <fstream>
#include <string>

#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>


#include <infiniband/verbs.h>


class hdRDMA{
	public:

		struct QPInfo {
			uint16_t lid;
			uint32_t qp_num;
		}__attribute__ ((packed));

		struct HeaderInfo {
			uint32_t header_len;
			uint16_t buff_type;
			uint16_t flags; // bit 0=first, 1=last
			uint32_t payload;
		}__attribute__ ((packed));
		
		         hdRDMA();
		         ~hdRDMA();
		    void CreateQP(void);
		    void Listen(int port);
		    void StopListening(void);
		    void Connect(std::string host, int port);
	       void SendQPInfo( int sockfd );
			 void ReceiveQPInfo( int sockfd );
			  int SetToRTS(void);
		uint32_t GetNpeers(void);
		    void SendFile(std::string srcfilename, std::string dstfilename);
		    void SendBuffer(uint8_t *buff, uint32_t buff_len);
			 void ReceiveBuffer(uint8_t *buff, uint32_t buff_len);
		    void PollCQ(void);

	protected:
		
		struct ibv_device *dev = nullptr;
		struct ibv_context *ctx = nullptr;
		struct ibv_device_attr attr;
		struct ibv_port_attr port_attr;
		struct ibv_qp_init_attr qp_init_attr;
		ibv_pd *pd = nullptr;
		uint32_t buff_len = 0;
		uint8_t *buff = nullptr;
		uint32_t num_buff_sections = 0;
		uint32_t buff_section_len = 0;
		struct ibv_mr *mr = nullptr;
		struct ibv_comp_channel *comp_channel = nullptr;
		struct ibv_cq *cq = nullptr;
		struct ibv_qp *qp = nullptr;
		struct ibv_recv_wr *bad_wr=nullptr;

		bool done = false;
		int server_sockfd = 0;
		std::thread *server_thread = nullptr;
		QPInfo qpinfo;
		QPInfo remote_qpinfo;
		uint32_t Npeers = 0;

		std::ofstream *ofs = nullptr;
		std::string ofilename;
		uint64_t ofilesize = 0;
		
};

