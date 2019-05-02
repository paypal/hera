package lib

import (
	"strconv"
	"testing"
)

/* this is a port of the Java test */
func TestScuttleID(t *testing.T) {
	key, err := strconv.ParseUint("1703900906402232986", 10, 64)
	if err != nil {
		t.Fatal("failed to parse:", err)
	}
	bytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		bytes[i] = byte(key & 0xFF)
		key >>= 8
	}
	scuttleID := Murmur3(bytes) % 1024
	if scuttleID != 470 {
		t.Fatalf("Expected scuttle_id: %d, instead got %d", 470, scuttleID)
	}
}
