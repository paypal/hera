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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>

#include "base64.h"
#include <string>



// encoder
const char * code64[2] = { 
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.",
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/" };

//decoder
const char ascii64[2][256] = { 
{
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,62,63,0, 52,53,54,55, 56,57,58,59, 60,61,0,0, 0,0,0,0,
 0,0,1,2, 3,4,5,6, 7,8,9,10, 11,12,13,14, 15,16,17,18, 19,20,21,22, 23,24,25,0, 0,0,0,0,
 0,26,27,28, 29,30,31,32, 33,34,35,36, 37,38,39,40, 41,42,43,44, 45,46,47,48, 49,50,51,0, 0,0,0,0,          // 128
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0
},
{
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,62, 0,0,0,63, 52,53,54,55, 56,57,58,59, 60,61,0,0, 0,0,0,0,
 0,0,1,2, 3,4,5,6, 7,8,9,10, 11,12,13,14, 15,16,17,18, 19,20,21,22, 23,24,25,0, 0,0,0,0,
 0,26,27,28, 29,30,31,32, 33,34,35,36, 37,38,39,40, 41,42,43,44, 45,46,47,48, 49,50,51,0, 0,0,0,0,          // 128
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0
}
};	

// encodes a single block
static void base64_encode_block(const char * src, char * dst, int src_len, int code)
{
	ulong buffer = 0;
	
	if( !src_len ) {
		return;
	}

	// copy src_len bytes of source into the buffer, always at buffer+1
	memcpy( (char *) (&buffer)+1, src, src_len );	
	// funky endian bug!
	buffer = htonl( buffer ) >> (6*(3-src_len));			
	for( int i=src_len; i>=0; i--) {
		// get the last guy
		dst[i] = code64[code][ buffer & 0x0000003f ];
		// shift
		buffer = ( buffer >> 6 ) & 0x00ffffff;
	} // for
} // base64_encode_block


// decodes a single block
static void base64_decode_block(const char * src, char * dst, int src_len, int code)
{
	ulong buffer = 0;
	unsigned char src_buf[4];
	int i;

	if( !src_len ) {
		return;	
	}

	memset( src_buf, 0, 4 );
	memcpy( src_buf, src, src_len );

	for( i=0;i<4;i++) {
		// or it into the last guy
		buffer = buffer << 6;	
		buffer = buffer | (long) (ascii64[ code ][ src_buf[i] ]);
	} // for
	buffer = ntohl( buffer );
	// copy the resulting bytes in
	memcpy( dst, (char *) &buffer+1, src_len-1 );		
} // base64_decode_block

void base64_decode(const std::string& _src, std::string& _dst)
{
	int dst_len = base64_decode_size(_src.length());
	_dst.resize(dst_len);
	base64_decode(_src.c_str(), (char*)_dst.c_str());
}

//decodes the string to data
void base64_decode( const char * src, std::string & dst )
{
	int dst_len = base64_decode_size(strlen(src));

	dst.resize(dst_len);
	base64_decode(src, (char*)dst.c_str());
} //base64_decode


void base64_encode(const std::string& _src, std::string& _dst)
{
	int dst_len = base64_size(_src.length());
	_dst.resize(dst_len);
	base64_encode(_src.c_str(), (char*)_dst.c_str(), _src.length());
}

//encodes the data
void base64_encode( const void * src, std::string & dst, int src_len )
{
	int dst_len = base64_size(src_len);
	dst.resize(dst_len);
	base64_encode((const char*)src, (char*)dst.c_str(), src_len);
} //base64_encode

// encodes the data
void base64_encode( const char * src, char * dst, int len ) {

	for( int i=0; i<len/3; i++ ) {
		// encode a single block
		base64_encode_block( src, dst, 3, 0 );
		// move up appropriately
		src+=3;
		dst+=4;
	} // for

	if(len%3) {
		base64_encode_block( src, dst, len%3, 0 );
		dst+= ((len%3)+1);
	}
	*dst = 0x0;					// null-term the string
	
} // base64_encode


//decodes the data
void base64_decode( const char * src, char * dst ) {
int len;
	
	len = strlen( src );	// get the length

	for( int i=0; i<len/4; i++ ) {		
		// decode a single block
		base64_decode_block( src, dst, 4, 0 );
		// move up appropriately
		src+=4;
		dst+=3;
	} // while
	base64_decode_block( src, dst, len%4, 0 );
	
} // base64_decode

// is this base64 char a valid one?
bool is_valid_paypal_base64( char _c ) {
	if( (_c >= 'A' && _c <= 'Z')  ||  (_c >= 'a' && _c <= 'z' ) || (_c >= '0' && _c <= '9') || ( _c == '.') || (_c == '-') ) {
		return true;
	} else {
		return false;
	} // if
} // is_valid_paypal_base64

bool validate_paypal_base64( const char *_src)
{
	int encoded_len = strlen(_src);

        if (encoded_len == 0) return false;

	for (int i = encoded_len - 1; i >= 0; --i)
	{
		if (!is_valid_paypal_base64( _src[i] )) {
		     return false;
		}
        }

        return true;
}

//
// calculate RFC based memory req's for base64 encoding
//
int base64_size_RFC( int src_size, bool with_newlines ) {
int k =0;
int crlf;
	// calculate the exact number of bytes
	if( src_size % 3 == 0 ) {
		k = src_size / 3 * 4;
	} else {
		k = ( src_size / 3 + 1 ) * 4;
	} // if
	// now, we need to account for the CRLF's necessary at the end of each line
	// which we do a little bit more sloppily
	if(with_newlines)
		crlf = ( k / 76 + 1 ) * 2;
	else
		crlf = 0;
	// and one byte for the zero-terminator
	return crlf + k + 1;
} // base64

//
// calculate no-new-line-based memory req's for base64 encoding, and no zero-terms
//
int base64_size( int src_size ) {
int k =0;
	// calculate the exact number of bytes
	if( src_size % 3 == 0 ) {
		k = src_size / 3 * 4;
	} else {
		k = ( src_size / 3 * 4 ) + ( src_size % 3 + 1 );
	} // if
	return k;
} // base64


// is this base64 char a valid one?
int base64_valid_RFC( char c ) {
	if( (c >= 'A' && c<='Z')  ||  (c >='a' && c<='z' ) || (c >='0' && c<='9') || ( c=='+') || (c=='/') ) {
		return 1;
	} else {
		return 0;
	} // if
} // base64_valid_RFC


int base64_decode_size_RFC( const char * src )
{
	int pad = 0;
	int garbage = 0;
	int encoded_len = strlen(src);

	// Don't count the '=' pad characters at the end for RFC base64 encoding
	// Don't count the '\n', '\r', or any garbage at the end for MIME encoding
	for (int i = encoded_len - 1; i >= 0; --i)
	{
		if (base64_valid_RFC(src[i]))
		{
			break;
		}

		if (src[i] == '=')
		{
			++pad;
		}
		else
		{
			++garbage;
		}
	}

	// RFC encoding with padding will always be a multiple of 4
	return ((encoded_len - garbage) * 3/4) - pad;
}

int base64_decode_size( const char * src )
{
	return strlen(src)*3/4;
}

int base64_decode_size( int src_size )
{
	return src_size*3/4;
}

//encodes the data RRC-exact
void base64_encode_RFC( const void * src, std::string & dst, int src_len, bool with_newlines)
{
	int dst_len = base64_size_RFC(src_len);
	dst.resize(dst_len);
	
	base64_encode_RFC((const char *) src, (char*)dst.c_str(), src_len, with_newlines);
	
} //base64_encode_RFC

// RFC-exact base64 encoder
void base64_encode_RFC( const char * src, char * dst, int len,bool with_newlines ) {
int cnt = 0;

	// go through the data
	for( int i=0; i<len/3; i++ ) {
		// encode a single block
		base64_encode_block( src, dst, 3, 1 );
		// move up appropriately
		src+=3;
		dst+=4;
		cnt++;
		if( cnt >= 19 ) {
			if(with_newlines){
				*(dst++) = '\r';
				*(dst++) = '\n';
			}//if
			cnt = 0;
		}
	} // for

	base64_encode_block( src, dst, len%3, 1 );
	dst+=((5-(len%3))*(len%3))/2;	//can you figure out what this does? ;-)
	switch(len%3){		
	case 1:
		*(dst++) = '=';
	case 2:
		*(dst++) = '=';			//
	case 0:
		*dst = 0x0;			//dst points the right way
	} // switch
} // base64_encode_RFC


/**
 * @brief base64 decoder done using the standard RFC
 *
 * @param _src - the input base64 buffer [IN]
 * @param _dst - the output decoded buffer [OUT]
 * @ret 
 */
int base64_decode_RFC( const char * _src, std::string &_dst ) {
	unsigned int buffer_len = base64_decode_size_RFC(_src);
	_dst.resize(buffer_len);
	char *dest_buf = (char*)_dst.c_str();
	int size_counter = 0;
	char block[4];
	int offset=0;
	memset( block, 0, 4 );	// clean out the decoder	
	while( *_src ) {
		// grab the data
		if( base64_valid_RFC( *_src )) {
			block[offset++] = *_src;
		}
		_src++;
		if( *_src==0x0 || offset == 4 ) {
			// Only offset values of 2,3, and 4 are usable, although
			//   we expect 4 since the encoding should be
			//   using the '=' padding characters
			// Offset of 0 might occur when the last _src was '\n'
			// Offset of 1 means the data is corrupt
			if (offset > 1)
			{
				base64_decode_block( block, dest_buf, offset, 1 );
				dest_buf += offset-1;
				size_counter += (offset-1);
			}
			offset = 0;
			memset( block, 0, 4 );	// clean out the decoder	
		} // if
	} // while
	return size_counter;
} // base64_decode_RFC


