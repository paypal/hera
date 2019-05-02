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

package lib

// Queue is interface for a queue implementation 
type Queue interface {
	// Len function tells how many elements are  
	Len() int
	// Push adds an element to the queue, at the end 
	Push(el interface{}) bool
	// PushFront adds an element to the queue, at the front - basically making this a stack
	PushFront(el interface{}) bool
	// Poll poss an element from the queue
	Poll() interface{}
	// Remove removes the element having the given value
	Remove(el interface{}) bool
	// ForEachRemove walks the entire list removeing elements satisfying the condition
	ForEachRemove(f func (interface{}) bool) int
}

// ringQueue implements Queue using internally a ring buffer.
// Note: this queue has disting elements, iot doesn't allow adding a duplicate entry 
type ringQueue struct {
	data []interface{}
	head, tail, capacity int
	// idmap helps to eficiently check if an element is in the list, so that Push/PushFront will not add a duplicate
	idmap map[interface{}]bool
}

// NewQueue creates a queue
func NewQueue() Queue {
	capacity := 128
	return &ringQueue{data: make([]interface{}, capacity), capacity: capacity, idmap: make(map[interface{}]bool)}
}

// Len function tells how many elements are  
func (q *ringQueue)Len() int {
	if q.tail >= q.head {
		return q.tail - q.head
	}
	return q.tail - q.head + q.capacity	
}

func (q *ringQueue)double() {
	data := q.data
	capacity := q.capacity 
	q.capacity *= 2
	q.data = make([]interface{}, q.capacity)
	idxs := q.head
	idxd := 0
	for idxs != q.tail {
		q.data[idxd] = data[idxs]
		idxd = idxd + 1 
		idxs = (idxs + 1) % capacity
	}
	q.head = 0
	q.tail = idxd
}

// Push adds an element to the queue, at the end 
func (q *ringQueue)Push(el interface{}) bool {
	if q.idmap[el] {
		return false
	}
	q.idmap[el] = true
	if q.Len() + 1 == q.capacity {
		q.double()
	}	
	q.data[q.tail] = el
	q.tail = q.incr(q.tail)
	return true
}

// PushFront adds an element to the queue, at the front - basically making this a stack
func (q *ringQueue)PushFront(el interface{}) bool{
	if q.idmap[el] {
		return false
	}
	q.idmap[el] = true
	if q.Len() + 1 == q.capacity {
		q.double()
	}
	q.head = q.decr(q.head)
	q.data[q.head] = el
	return true
}

// Poll poss an element from the queue
func (q *ringQueue)Poll() interface{} {
	if q.Len() == 0 {
		return nil
	}
	el := q.data[q.head]
	q.head = q.incr(q.head)
	delete(q.idmap, el)
	return el
}

func (q *ringQueue)decr(pos int) int{
	return ((pos + q.capacity - 1) % q.capacity)
}

func (q *ringQueue)incr(pos int) int{
	return ((pos + 1) % q.capacity)
}

func (q *ringQueue)remove(pos int) {
	delete(q.idmap, q.data[pos])
	next := q.incr(pos)
	for next != q.tail {
		q.data[pos] = q.data[next]
		pos = next
		next = q.incr(next)
	}
	q.tail = q.decr(q.tail)
}

// Remove removes the element having the given value
func (q *ringQueue)Remove(el interface{}) bool {
	pos := q.head
	for pos != q.tail {
		if el == q.data[pos] {
			q.remove(pos)
			return true
		}
		pos = q.incr(pos)
	}
	return false
}

// ForEachRemove walks the entire list removeing elements satisfying the condition
func (q *ringQueue)ForEachRemove(f func (interface{}) bool) int {
	cnt := 0
	pos := q.head
	for pos != q.tail {
		if f(q.data[pos]) {
			q.remove(pos)
			cnt++
		} else {
			pos = q.incr(pos)
		}
	}
	return cnt
}
