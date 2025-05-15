package snappyBench

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	go_snappy "github.com/golang/snappy"
	klaus_spot_snappy "github.com/klauspost/compress/snappy"
)

// Define the characters that can be used in the random text
var characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
var data []byte

func init() {
	rand.Seed(time.Now().UnixNano())

	// Define the length of the random text
	textLength := 150 * 1024 // Adjust this value to generate text of desired length

	// Generate the random text
	var sb strings.Builder
	for i := 0; i < textLength; i++ {
		sb.WriteByte(characters[rand.Intn(len(characters))])
	}
	data = []byte(sb.String())
}

func BenchmarkGolangSnappy(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = go_snappy.Encode(nil, data)
	}
}

func BenchmarkKlausPostSnappy(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = klaus_spot_snappy.Encode(nil, data)
	}
}

func BenchmarkGolangSnappyDecompression(b *testing.B) {
	compressedData := go_snappy.Encode(nil, data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = go_snappy.Decode(nil, compressedData)
	}
}

func BenchmarkKlausPostSnappyDecompression(b *testing.B) {
	compressedData := klaus_spot_snappy.Encode(nil, data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = klaus_spot_snappy.Decode(nil, compressedData)
	}
}
