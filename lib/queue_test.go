package lib

import (
	"log"
	"testing"
)

const (
	pushFailure = "push failure"
	idemFailure = "idempotency failure"
)

func TestDempotency(t *testing.T) {
	gOpsConfig = &OpsConfig{numWorkers: 10}
	var queue = NewQueue()
	wa := NewWorker(0, wtypeRW, 0, 0, "cloc", nil)
	wb := NewWorker(0, wtypeRW, 0, 0, "cloc", nil)
	wc := NewWorker(0, wtypeRW, 0, 0, "cloc", nil)

	queue.Push(wa) // wa
	if queue.Len() != 1 {
		t.Error(pushFailure)
	}
	log.Println("wa ", queue, "len=", queue.Len())
	queue.Push(wb) // wb wa
	if queue.Len() != 2 {
		t.Error(pushFailure)
	}
	log.Println("wb wa ", queue, "len=", queue.Len())
	queue.Push(wc) // wc wb wa
	if queue.Len() != 3 {
		t.Error(pushFailure)
	}
	log.Println("wc wb wa ", queue, "len=", queue.Len())
	queue.Push(wc)
	if queue.Len() != 3 {
		t.Error(idemFailure)
	}
	log.Println("(wc) wc wb wa ", queue, "len=", queue.Len())
	queue.Poll() // wc wb
	if queue.Len() != 2 {
		t.Error("poll failure")
	}
	log.Println("wc wb ", queue, "len=", queue.Len())
	queue.Push(wc)
	if queue.Len() != 2 {
		t.Error(idemFailure)
	}
	log.Println("(wc) wc wb ", queue, "len=", queue.Len())
	queue.Push(wa) // wa wc wb
	if queue.Len() != 3 {
		t.Error(pushFailure)
	}
	log.Println("wa wc wb ", queue, "len=", queue.Len())
	queue.Remove(wc) // wa wb
	if queue.Len() != 2 {
		t.Error("remove failure")
	}
	log.Println("wa wb ", queue, "len=", queue.Len())
	queue.Push(wa)
	if queue.Len() != 2 {
		t.Error(idemFailure)
	}
	log.Println("(wa) wa wb ", queue, "len=", queue.Len())
	queue.Push(wc) // wc wa wb
	if queue.Len() != 3 {
		t.Error(pushFailure)
	}
	log.Println("wc wa wb ", queue, "len=", queue.Len())
	queue.PushFront(wc)
	if queue.Len() != 3 {
		t.Error(idemFailure)
	}
	log.Println("wc wa wb (wc) ", queue, "len=", queue.Len())
	queue.Remove(wb) // wc wa
	if queue.Len() != 2 {
		t.Error("remove failure")
	}
	log.Println("wc wa ", queue, "len=", queue.Len())
	queue.PushFront(wb) // wc wa wb
	if queue.Len() != 3 {
		t.Error("push front failure")
	}
	log.Println("wc wa wb ", queue, "len=", queue.Len())

}

func TestQueueRemove(t *testing.T) {
	var queue = NewQueue()
	queue.PushFront(9)
	queue.PushFront(10)
	queue.PushFront(4)
	queue.PushFront(3)
	queue.PushFront(11)
	queue.PushFront(2)
	queue.PushFront(1)
	queue.PushFront(8)
	queue.PushFront(7)
	queue.PushFront(0)
	queue.PushFront(6)
	queue.PushFront(5)
	if queue.Len() != 12 {
		t.Error(pushFailure)
	}
	rmfunc := func(el interface{}) bool {
		item := el.(int)
		t.Logf("For each: %d", item)
		return item >= 5
	}
	queue.ForEachRemove(rmfunc)
	if queue.Poll().(int) != 0 {
		t.Error("Poll expected 0")
	}
	queue.PushFront(101)
	if queue.Poll().(int) != 101 {
		t.Error("Poll expected 101")
	}
	if queue.Poll().(int) != 1 {
		t.Error("Poll expected 1")
	}
	if queue.Poll().(int) != 2 {
		t.Error("Poll expected 2")
	}
	if queue.Poll().(int) != 3 {
		t.Error("Poll expected 3")
	}
	queue.Push(102)
	if queue.Poll().(int) != 4 {
		t.Error("Poll expected 4")
	}
	if queue.Poll().(int) != 102 {
		t.Error("Poll expected 102")
	}
	if queue.Poll() != nil {
		t.Error("Expect empty queue")
	}
}
