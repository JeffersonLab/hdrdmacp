
#include <cstdint>
#include <thread>
#include <fstream>
#include <string>
#include <list>
#include <deque>
#include <atomic>
#include <mutex>
#include <condition_variable>

#if __GNUC__
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#endif

#ifdef _MSC_VER
#ifndef WIN32
#define WIN32
#endif
#include <Winsock2.h>
#endif

#include <infiniband/verbs.h>

#include "hdRDMAThread.h"

#include "IhdRDMA.h"

struct hdBuffer
{
	uint64_t buff_len = 0;
	uint8_t *buff = nullptr;
	uint64_t num_buff_sections = 0;
	uint64_t buff_section_len = 0;
	struct ibv_mr *mr = nullptr;
};

class hdRDMA final : public hdrdma::IhdRDMA {
	public:
		
		         hdRDMA(const hdrdma::config &config);
		         ~hdRDMA() override;
		virtual bool Good() const override { return dev != nullptr; }
		virtual void Listen(int port) override;
		virtual void StopListening(void) override;
		virtual void Connect(std::string host, int port) override;
		uint32_t GetNpeers(void);
		    void GetBuffers( std::vector<hdRDMAThread::bufferinfo> &buffers, int Nrequested=4 );
		    void ReturnBuffers( std::vector<hdRDMAThread::bufferinfo> &buffers );
		virtual void SendFile(std::string srcfilename, std::string dstfilename, bool delete_after_send=false, bool calculate_checksum=false, bool makeparentdirs=false) override;
		virtual void Poll(void) override;
		virtual void Join(void) override;

		virtual uint64_t TotalBytesReceived() const override { return total_bytes_received; }
		std::atomic_ullong total_bytes_received = 0;

		std::string DecodePath(const std::string_view& p) const;
		std::shared_ptr<hdrdma::IPathDecoder> PathDecoder;

		struct ibv_device *dev = nullptr;
		struct ibv_context *ctx = nullptr;
		int    port_num = 1;
		struct ibv_device_attr attr;
		struct ibv_port_attr port_attr;
		ibv_pd *pd = nullptr;
		std::vector<hdBuffer> buffers;
		std::deque<hdRDMAThread::bufferinfo> buffer_pool;
		std::mutex buffer_pool_mutex;
		std::condition_variable buffer_pool_cond;

		bool done = false;
		SOCKET server_sockfd = 0;
		std::thread *server_thread = nullptr;
		std::thread *ack_thread = nullptr;
		uint32_t Nconnections = 0;
		
		std::unique_ptr<hdRDMAThread> hdthr_client = nullptr;
		std::map<std::thread*, hdRDMAThread*> threads;
		std::mutex threads_mtx;

		std::atomic<uint64_t> Ntransferred;
		uint64_t Ntransferred_last = 0;
		std::chrono::high_resolution_clock::time_point t_last;

		std::string remote_addr;
};

