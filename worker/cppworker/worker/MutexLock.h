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
/* Implement a monitor lock. This is used by HBSender
 * This helps make HBSender code robust and readable
 *
*/

#ifndef _Mutext_Lock_H
#define _Mutext_Lock_H 

#include <pthread.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>

#include "utility/Assert.h"

// As per man page some of mutex/cond APIs are not interruptible, 
// however it is not assumed here and that's better
//

#define synchronize(lock) SmartLock dummy(lock)

class Synchronizable {
	private:
		pthread_mutex_t m_lock;
		pthread_cond_t  m_cond;

	public:
		Synchronizable() {
			int rc = pthread_mutex_init(&m_lock, 0);
			ASSERT(!rc);
			rc = pthread_cond_init(&m_cond, 0);
			ASSERT(!rc);
		}

		void acquire() {
			int rc;
			do {
				rc = pthread_mutex_lock( &m_lock );
			} while (rc !=0 && errno == EINTR );

			ASSERT(!rc);
		}

		void release() {
			int rc;
			do {
				rc = pthread_mutex_unlock( &m_lock );
			} while (rc !=0 && errno == EINTR );

			ASSERT(!rc);
		}

		void wait(int _timeout_sec = -1) {
			wait(&m_cond, _timeout_sec);
		}


		void wait(pthread_cond_t* _cond, int _timeout_sec = -1) {
			if ( _timeout_sec == -1 ) {
				int rc = pthread_cond_wait(_cond, &m_lock);
				ASSERT(!rc);
			} else {
				struct timeval now;
				gettimeofday(&now, NULL);

				struct timespec abs_timeout;
				abs_timeout.tv_sec = now.tv_sec + _timeout_sec;
				abs_timeout.tv_nsec = now.tv_usec * 1000;
				pthread_cond_timedwait(_cond, &m_lock, &abs_timeout);
				// if interrupted, throw exception
			}
		}

		void notify() {
			notify(&m_cond);
		}

		void notify(pthread_cond_t* _cond) {
			int rc;
			do {
				rc = pthread_cond_signal(_cond);
			} while (rc !=0 && errno == EINTR );

			ASSERT(!rc);
		}

		void notify_all() {
			notify_all(&m_cond);
		}

		void notify_all(pthread_cond_t* _cond) {
			int rc;
			do {
				rc = pthread_cond_broadcast(_cond); 
			} while (rc !=0 && errno == EINTR );

			ASSERT(!rc);
		}

		~Synchronizable() {
			pthread_mutex_destroy(&m_lock);
			pthread_cond_destroy(&m_cond);
		}	
};

class SmartLock {
	public :
		SmartLock(Synchronizable* _lock) {
			m_lock = _lock;
			m_lock->acquire();
		}

		~SmartLock() {
			m_lock->release();
		}
	private:
		Synchronizable* m_lock;
};

#endif

