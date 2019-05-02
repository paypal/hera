// Copyright 2019 PayPal Inc.
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

package cal

type ringBuffer struct {
	mRing     []byte
	mCapacity int // number of bytes on the ringbuffer (fixed)
	mUsed     int // number of bytes used
	mHead     int // index of the first byte in use
	mTail     int // index of the last byte in use + 1. or index of the first free byte
}

func (rb *ringBuffer) Init(_size int) error {
	rb.mRing = make([]byte, _size)
	rb.mCapacity = _size
	rb.Clear()

	return nil
}

func (rb *ringBuffer) Capacity() int {
	return rb.mCapacity
}

func (rb *ringBuffer) FreeCapacity() int {
	return rb.mCapacity - rb.mUsed
}

func (rb *ringBuffer) Clear() {
	rb.mHead = 0
	rb.mTail = 0
	rb.mUsed = 0
}

/**
 * write _size number of bytes from _src to ringbuffer starting at pTail + 1
 */
func (rb *ringBuffer) WriteData(_src []byte, _size int) bool {
	if _size == 0 {
		return true
	}

	//
	// add data only if free space is available
	//
	if rb.FreeCapacity() < _size {
		return false
	}

	//
	// copy whatever possible till end
	//
	var writeCount = _size
	if rb.mCapacity-rb.mTail <= _size {
		writeCount = rb.mCapacity - rb.mTail
	}
	//
	// passing a subslice by value
	// it alters byte values in mRing (since copy of location pointer still points to the
	// same memory location) without changing header information in mRing (its size cap)
	//
	copy(rb.mRing[rb.mTail:], _src[0:writeCount])
	rb.mTail += writeCount
	rb.mUsed += writeCount

	//
	// wrap around if needed
	//
	if rb.mTail == rb.mCapacity {
		rb.mTail = 0
	}

	//
	// if data is still remaining, do another write_data
	//
	if _size-writeCount > 0 {
		leftover := _src[writeCount:]
		return rb.WriteData(leftover, _size-writeCount)
	}
	return true
}

func (rb *ringBuffer) CopyData(_dest []byte, _size int) bool {
	if _size == 0 {
		return true
	}

	//
	// add data only if free space is available
	//
	if rb.mUsed < _size {
		return false
	}

	//
	// copy whatever possible till end
	//
	var readCount = _size
	if rb.mCapacity-rb.mHead <= _size {
		readCount = rb.mCapacity - rb.mHead
	}
	copy(_dest[0:readCount], rb.mRing[rb.mHead:(rb.mHead+readCount)])

	//
	// wrap around and read more if needed
	//
	if _size-readCount > 0 {
		//
		// copy stops at the smaller elements. if _dest.cap < _size, only copy up to cap.
		//
		copy(_dest[readCount:], rb.mRing[:(_size-readCount)])
	}
	return true
}

/**
 * "remove" _size number of bytes starting from mHead by relocating mHead.
 * if _size wraps around keep moving until _size number of bytes are counted.
 * mHead can not move pass mTail since we check (rb.m_iUsed < _size)
 */
func (rb *ringBuffer) RemoveData(_size int) bool {
	if _size == 0 {
		return true
	}

	if rb.mUsed < _size {
		return false
	}

	var removeCount = _size
	if rb.mCapacity-rb.mHead <= _size {
		removeCount = rb.mCapacity - rb.mHead
	}
	rb.mHead += removeCount
	rb.mUsed -= removeCount

	if rb.mHead == rb.mCapacity {
		rb.mHead = 0
	}

	if (_size - removeCount) > 0 {
		return rb.RemoveData(_size - removeCount)
	}
	return true
}
