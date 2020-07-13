// Copyright 2020 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "CalRingBuffer.h"
#include <string.h>

CalRingBuffer::CalRingBuffer (unsigned int size, CalLog *logger)
: m_iCapacity (size)
, m_iUsed (0)
{
	m_pStart = new char[size];
	m_pEnd = m_pStart+size; //sentinel
	m_pHead = m_pTail = m_pStart;
	m_logger = logger;
}
CalRingBuffer::~CalRingBuffer ()
{
	delete [] m_pStart;
}
unsigned int CalRingBuffer::capacity () const
{
	return m_iCapacity;
}
unsigned int CalRingBuffer::free_capacity () const
{
	return m_iCapacity-m_iUsed;
}

unsigned int CalRingBuffer::used_capacity () const
{
	return m_iUsed;
}

bool CalRingBuffer::write_data (char const *pData, unsigned int size)
{
	if( size == 0)
		return true;

	//  add data only if free space is available
	if (free_capacity()<size)
		return false;

	//  copy whatever possible till end
	unsigned int write_count = (unsigned int)(m_pEnd-m_pTail) > size ? size : m_pEnd-m_pTail;
	memcpy (m_pTail, pData, write_count);
	m_pTail+=write_count;
	m_iUsed+=write_count;

	//  wrap around if needed
	if (m_pTail==m_pEnd)
		m_pTail=m_pStart;

	//  if data is still remaining, do another write_data
	if (size-write_count>0)
		return write_data(pData+write_count, size-write_count);  
	return true;
}

bool CalRingBuffer::remove_data (unsigned int size)
{
	if( size == 0)
		return true;

	if (m_iUsed<size)
		return false;


	unsigned int remove_count = (unsigned int)(m_pEnd-m_pHead) > size ? size : m_pEnd-m_pHead;
	m_pHead+=remove_count;
	m_iUsed-=remove_count;
	
	if (m_pHead==m_pEnd)
		m_pHead=m_pStart;
	
	if (size-remove_count>0)
		return remove_data (size-remove_count);
	return true;
}

bool CalRingBuffer::copy_data (char *pData, unsigned int size) const
{
	if( size == 0)
		return true;
	
	if (m_iUsed<size)
		return false;


	unsigned int read_count = (unsigned int)(m_pEnd-m_pHead) > size ? size : m_pEnd-m_pHead;
	memcpy (pData, m_pHead, read_count);

	//  wrap around and read more if needed
	if (size-read_count>0)
	{
		memcpy (pData+read_count, m_pStart, size-read_count);
	}
	return true;
}

bool CalRingBuffer::clear()
{
	//get used space size from the ring buffer
	int size = used_capacity();
	//remove all the used data from the ring buffer
	remove_data (size);

	return true;
}
