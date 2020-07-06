#ifndef __CALRINGBUFFER_H
#define __CALRINGBUFFER_H
#include "CalLog.h"

class CalRingBuffer 
{
	public:
		CalRingBuffer (unsigned int size, CalLog *logger);
		~CalRingBuffer ();
		
		unsigned int capacity () const;
		unsigned int free_capacity () const;
		unsigned int used_capacity () const;

		bool write_data (char const *pData, unsigned int size);
		bool remove_data (unsigned int size);
		bool copy_data (char *pData, unsigned int size) const;
		bool clear();

	private:
		char *m_pStart, *m_pEnd, *m_pHead, *m_pTail;
		const unsigned int m_iCapacity;
		unsigned int m_iUsed;
		CalLog* m_logger;
};

#endif
