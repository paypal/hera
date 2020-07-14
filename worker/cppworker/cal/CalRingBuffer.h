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
