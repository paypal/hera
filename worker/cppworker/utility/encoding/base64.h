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
#ifndef _BASE64_H_
#define _BASE64_H_

#include <string>

// more advanced interfaces
void base64_encode(const std::string& _src, std::string& _dst);

// String class base encoding.
void base64_encode( const void * src, std::string & dst, int src_len );
void base64_decode( const char * src, std::string & dst );


//
// This a base64 encoding with a slightly modified encoder set
//
//
// encodes the data
void base64_encode( const char * src, char * dst, int len );
//decodes the data
void base64_decode( const char * src, char * dst );
// NEW: the non-rfc version, does not account for new lines
int base64_size( int src_size );
int base64_decode_size( const char * src );
int base64_decode_size( int src_size );
// is the paypal base64 char valid one?
bool is_valid_paypal_base64( char _c );
// validate this paypal base64 string using base64_valid_paypal which is
// defined in base64.cpp:
// return true if it is a valid paypal base64 string;
// return false if it is not a valid paypal base64 string.
bool validate_paypal_base64( const char *src);

//
//
//
// This is base64 encoding with a precisely-to-RFC spec.
//
//
//
int base64_size_RFC( int src_size, bool with_newlines = true );
// assumes no newlines
int base64_decode_size_RFC( const char * src );
void base64_encode_RFC( const char * src, char * dst, int len, bool with_newlines=true );

// String base RFC encoding function
void base64_encode_RFC( const void * src, std::string & dst, int src_len, bool with_newlines=true );

// String base RFC function for decryption!
int base64_decode_RFC( const char * _src, std::string &_dst );
#endif
