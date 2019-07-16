
#include <cstdint>
#include <thread>
#include <fstream>
#include <string>
#include <list>
#include <deque>
#include <atomic>

#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <zlib.h>

#include <infiniband/verbs.h>

#include <hdRDMAThread.h>


class hdRDMA{
	public:
		
		         hdRDMA();
		         ~hdRDMA();
		    void CreateQP(void);
		    void Listen(int port);
		    void StopListening(void);
		    void Connect(std::string host, int port);
		uint32_t GetNpeers(void);
		    void GetBuffers( std::vector<hdRDMAThread::bufferinfo> &buffers, int Nrequested=4 );
		    void ReturnBuffers( std::vector<hdRDMAThread::bufferinfo> &buffers );
		    void SendFile(std::string srcfilename, std::string dstfilename);
		    void Poll(void);

		
		struct ibv_device *dev = nullptr;
		struct ibv_context *ctx = nullptr;
		int    port_num = 1;
		struct ibv_device_attr attr;
		struct ibv_port_attr port_attr;
		ibv_pd *pd = nullptr;
		uint64_t buff_len = 0;
		uint8_t *buff = nullptr;
		uint64_t num_buff_sections = 0;
		uint64_t buff_section_len = 0;
		struct ibv_mr *mr = nullptr;
		std::deque<hdRDMAThread::bufferinfo> buffer_pool;
		std::mutex buffer_pool_mutex;

		bool done = false;
		int server_sockfd = 0;
		std::thread *server_thread = nullptr;
		uint32_t Nconnections = 0;
		
		hdRDMAThread *hdthr_client = nullptr;
		std::map<std::thread*, hdRDMAThread*> threads;

		std::atomic<uint64_t> Ntransferred;
		uint64_t Ntransferred_last = 0;
		std::chrono::high_resolution_clock::time_point t_last;
};

