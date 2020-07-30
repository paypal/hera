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
#ifndef __CALTIME_H
#define __CALTIME_H

#include <sys/time.h>
#include <unistd.h>

#define kMinDuration 0
#define kMaxDuration 999999

//
//	Class CalTimer
//
typedef unsigned long long Cal_uint64;
typedef long long Cal_int64;

class CalTimer
{
public:
	CalTimer();
		// Construct a timer. This calls Reset().

	void Reset();
		// Reset the timer to 0 -- i.e. the next call to any method above
		// will the time elapsed from this call to Reset.

	unsigned TickFrequency()
		{ return kTicksPerSecond; }

	unsigned Ticks();
		// Ticks elapsed since the last reset
		// Note that it can wrap around back to zero, but
		// only after a period > 1 day, assuming kTicksPerMillisecond is 1 or 10

	unsigned Milliseconds();
		// The number of milliseconds elapsed since the last reset
		// Identical to Tick of kTicksPerMillisecond==1

	unsigned Centiseconds();
		// The number of centiseconds elapsed since the last reset
		// Identical to Tick of kTicksPerMillisecond==1

	double Seconds();
		// The number of (fractional) seconds elapsed since the last reset
		// This is at the full precision of the underlying timer
		
	double Duration()
		{ return Seconds(); }
		// This is the old interface, kept for backwards compatibility
		
	Cal_uint64 Microseconds();
		// The number of microsecods elapsed since the last reset

private:
	Cal_uint64 Cycles();
		// The number of Machine cycles since the last reset

private:
	Cal_uint64	mHighResFreq;
		// Cycles per second
	Cal_uint64	mHighResTimeBase;
		// High resolution time at Tick==0

public:
	enum
	{
		kTicksPerMillisecond = 10,	// should be 1 or 10

		kMillisecondsPerSecond = 1000,
		kMicrosecondsPerSecond = 1000 * 1000,
		kTicksPerSecond = kMillisecondsPerSecond * kTicksPerMillisecond,
		kSecondsPerReset = 30,
		kResetDelta = kTicksPerSecond * kSecondsPerReset,

		kCentisecondsPerSecond = 100,
		kMillisecondsPerCentisecond = 10,
		kMicrosecondsPerCentisecond = kMillisecondsPerCentisecond * 1000,
		kTicksPerCentisecond = kTicksPerSecond / kCentisecondsPerSecond,

		kSecondsPerMinute = 60,
		kMinutesPerHour = 60,
		kHoursPerDay = 24,
		kSecondsPerHour = kSecondsPerMinute * kMinutesPerHour,
		kSecondsPerDay = kSecondsPerHour*kHoursPerDay,
		kCentisecondsPerMinute = kSecondsPerMinute * kCentisecondsPerSecond,
		kCentisecondsPerHour = kSecondsPerHour * kCentisecondsPerSecond,
		kCentisecondsPerDay = kSecondsPerDay * kCentisecondsPerSecond,
		kMillisPerDay = kSecondsPerDay * kMillisecondsPerSecond,
		kTicksPerDay = kMillisPerDay * kTicksPerMillisecond
	};
};

//
//	Class CalDayTime
//

class CalDayTime : private CalTimer
{
public:
	CalDayTime();

	unsigned CentisecondsOfDay();
		// Centiseconds elapsed since midnight

	void TimeOfDay(char* time);
		// return the current time of day as C string in format "HH:MM:SS.cc"

	static void TimeOfDay(unsigned centisecondsOfDay, char* time);
		// Given a time of day, return as C string in format "HH:MM:SS.cc"

	static unsigned CentisecondsFromTime(const char* time);
		// Given a time string in format "HH:MM:SS.cc", return centisecondsOfDay


private:
	void ResynchTimeOfDay();
	unsigned 		mReferenceCentiseconds;
		// Time of day at last Resync, in centiseconds past midnight

	unsigned		mNextResynch;
		// Time of day, in centiseconds, when we will next resynch
		// We do it once per hour. The main reason for doing it this
		// often is because of daylight savings time changes

private:
	void UpdateTimeOfDay();
	unsigned 		mLastTimeCentiseconds;
		// The time of day in centiseconds for the time string
	char			mLastTimeString[12];
		// "HH:MM:SS.cc\0"
};

class CalTimeOfDay
{
private:
	CalTimeOfDay() {}

public:
	static char* Now(char* buffer);
};

class CalMicrosecondTimer
{
public:
	CalMicrosecondTimer() { Reset(); }

	double	Duration();
		// return time duration since last Reset.
		// Units are milliseconds but the timer will yield microseconds
		//    or better precision if the host OS supports that precision
		// Note that each timer is Reset when it is constructed

	void	Reset();
		// Reset the timer to 0 -- i.e. the next call to Duration
		// will the time elapsed from this call to Reset.

public:
	static char* PrivFormatDuration(char* buffer, double d);
		// string representation of the value in milliseconds
		// fractional milliseconds will be given only to achieve
		// 3 digits of precision, i.e. ".ddd", "d.dd", "dd.d", "ddd",
		// not "ddd.ddd"

private:
	struct timeval		mBegin;
};

#endif

