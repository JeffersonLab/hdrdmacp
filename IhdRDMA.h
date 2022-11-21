#pragma once

#include <memory>
#include <string>

#ifdef _MSC_VER
#	define HDRMDA_DLL_EXPORT	__declspec(dllexport)
#	define HDRMDA_DLL_IMPORT	__declspec(dllimport)
#else
#	define HDRMDA_DLL_EXPORT	__attribute__((__visibility__("default")))
#	define HDRMDA_DLL_IMPORT
#endif

#ifdef BUILDING_HDRDMA
#	define HDRDMA_DLL			HDRMDA_DLL_EXPORT
#else
#	define HDRDMA_DLL			HDRMDA_DLL_IMPORT
#endif

namespace hdrdma
{
	class IhdRDMA;

	struct config
	{
		config(size_t buffer_section_sz, int buffer_section_count) : BufferSectionSize(buffer_section_sz), BufferSectionCount(buffer_section_count) {}

		const size_t BufferSectionSize;
		const size_t BufferSectionCount;
	};
}

extern "C"
{
	// Raw pointers. You probably don't want to use these.
	HDRDMA_DLL hdrdma::IhdRDMA* hdrdma_allocate(const hdrdma::config& config);
	HDRDMA_DLL void hdrdma_free(hdrdma::IhdRDMA*);
}

namespace hdrdma
{
	class IhdRDMA {
	public:
		virtual ~IhdRDMA() {}

		virtual void Listen(int port) = 0;
		virtual void StopListening(void) = 0;
		virtual void Connect(std::string host, int port) = 0;
		virtual void SendFile(std::string srcfilename, std::string dstfilename, bool delete_after_send = false, bool calculate_checksum = false, bool makeparentdirs = false) = 0;
		virtual void Poll(void) = 0;
		virtual void Join(void) = 0;

		virtual uint64_t TotalBytesReceived() const = 0;
	};

	// Wrappers. You probably want to use these.
	static std::shared_ptr<IhdRDMA> Create(const hdrdma::config& config)
	{
		return std::shared_ptr<IhdRDMA>(hdrdma_allocate(config), hdrdma_free);
	}
}
