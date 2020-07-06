// CalTime.cpp

#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>

#include "CalTime.h"

char* CalTimeOfDay::Now(char* buffer)
{
	static CalDayTime gDayTime;
	gDayTime.TimeOfDay(buffer);
	return buffer;
}

double	CalMicrosecondTimer::Duration()
{
	const double kMillisecondsPerSecond = 1000.0;
	double duration = 0;

	struct timeval aEnd;
	::gettimeofday (&aEnd, NULL);
	duration = (aEnd.tv_sec - mBegin.tv_sec) * kMillisecondsPerSecond +
			(aEnd.tv_usec - mBegin.tv_usec) / kMillisecondsPerSecond;

	return duration;
}

void CalMicrosecondTimer::Reset ()
{
	::gettimeofday (&mBegin, NULL);
}

#define intToDigit(i) ('0' + i)

static int NumDigits(int i)
{
	int result = 0;
	while (i > 0)
	{
		i = i/10;
		++result;
	}
	return result;
}

char* CalMicrosecondTimer::PrivFormatDuration(char* buffer, double d)
{
	int i;
	int f;

	char* start = buffer;

	if (d < 0.05)
	{
		*buffer++ = '0';
	}
	else if (d >= kMaxDuration)
	{
		for (i=0; i<6; i++)
		{
			*buffer++ = '9';
		}
	}
	else if (d < 1)
	{
		i = int(d*10 + 0.5);
		if (i == 10)
		{
			*buffer++ = '1'; 
		}
		else
		{
			*buffer++ = '.';
			*buffer++ = intToDigit(i);
		}
	}
	else if (d < 10) 
	{
		i = int(d + 0.05);
		if (i == 10)
		{
			*buffer++ = '1'; 
			*buffer++ = '0'; 
		}
		else
		{
			//MyAssert(i < 10);
			f = int((d-i)*10);
			//MyAssert(f >= 0);
			//MyAssert(f < 10);
			*buffer++ = intToDigit(i);
			if (f)
			{
				*buffer++ = '.';
				*buffer++ = intToDigit(f);
			}
		}
	}
	else
	{
		// d >= 10.0
		// We will drop the fraction entirely and just output the integer part
		i = int(d + 0.5);
		int digits = NumDigits(i);
		buffer += digits;
		while (digits > 0)
		{
			--digits;
			start[digits] = intToDigit(i % 10);
			i = i/10;
		}
	}
	*buffer = 0;	// in C, we need NUL termination byte

	//MyAssert(buffer - start <= 6);

	return start;
}

//
//	Class CalTimer
//
CalTimer::CalTimer()
{
	mHighResFreq = kMicrosecondsPerSecond;
	Reset();
}

void CalTimer::Reset()
{
	struct timeval tv;
	::gettimeofday(&tv, 0);
	mHighResTimeBase = Cal_uint64(tv.tv_sec)*kMicrosecondsPerSecond + tv.tv_usec;
}

unsigned CalTimer::Ticks()
{
	return (unsigned int) (Cycles()*kTicksPerSecond / mHighResFreq);
}

unsigned CalTimer::Milliseconds()
{
	return (unsigned int) (Cycles()*kMillisecondsPerSecond / mHighResFreq);
}

unsigned CalTimer::Centiseconds()
{
	return (unsigned int) (Cycles()*kCentisecondsPerSecond / mHighResFreq);
}

double CalTimer::Seconds()
{
	return double(Cal_int64(Cycles())) / Cal_int64(mHighResFreq);
}

Cal_uint64 CalTimer::Microseconds()
{
	return Cycles()*kMicrosecondsPerSecond / mHighResFreq;
}

Cal_uint64 CalTimer::Cycles()
{
	Cal_uint64 now;
	struct timeval tv;
	::gettimeofday(&tv, 0);
	now = Cal_uint64(tv.tv_sec)*kMicrosecondsPerSecond + tv.tv_usec;
	return now-mHighResTimeBase;
}

//
//	Class CalDayTime
//

CalDayTime::CalDayTime()
{
	ResynchTimeOfDay();
}

static inline char ValToChar(unsigned int i)
{
//	MyAssert(i < 10);
	return i + '0';
}

static inline unsigned CharToVal(char c)
{
//	MyAssert(c >= '0');
//	MyAssert(c <= '9');
	return c - '0';
}

void CalDayTime::ResynchTimeOfDay()
{
	Reset();
	mLastTimeCentiseconds = 0;
	strcpy(mLastTimeString, "00:00:00.00");
	

	struct timeval tv;
	::gettimeofday(&tv, 0);
	struct tm* result = ::localtime(&tv.tv_sec);
	
	unsigned centiseconds = tv.tv_usec/kMicrosecondsPerCentisecond;
	mReferenceCentiseconds = ((result->tm_hour*kMinutesPerHour+result->tm_min)*kSecondsPerMinute+result->tm_sec) 
	   * kCentisecondsPerSecond 
	  + centiseconds;
	
//	const unsigned kResyncPeriod = kCentisecondsPerHour;
	const unsigned kResyncPeriod = kCentisecondsPerSecond;	// for debugging, use this period
	unsigned mCurrentHour = mReferenceCentiseconds / kResyncPeriod;
	mNextResynch = (mCurrentHour+1) * kResyncPeriod;
}

unsigned CalDayTime::CentisecondsOfDay()
{
	unsigned centisecondsOfDay = Centiseconds() + mReferenceCentiseconds;
	if (centisecondsOfDay >= mNextResynch)
	{
		ResynchTimeOfDay();
		centisecondsOfDay = mReferenceCentiseconds;
	}
	return centisecondsOfDay;
}

static inline unsigned ComponentToVal(const char* component)
{
	return CharToVal(component[0])*10 + CharToVal(component[1]);
}

static unsigned UpdateComponent(char* component, unsigned delta, unsigned modulus)
{
	// Update just one component of a HH:MM:SS.CC string
	unsigned offset = delta % modulus;
	delta /= modulus;

	unsigned currentVal = ComponentToVal(component);
	currentVal += offset;
	if (currentVal >= modulus)
	{
		delta += currentVal / modulus;
		currentVal = currentVal % modulus;
	}

	component[1] = ValToChar(currentVal % 10);
	component[0] = ValToChar(currentVal / 10);

	return delta;
}

void CalDayTime::UpdateTimeOfDay()
{
	unsigned centiseconds = CentisecondsOfDay();

	unsigned delta = centiseconds - mLastTimeCentiseconds;
	mLastTimeCentiseconds = centiseconds;

	if (!delta) return;
	delta = UpdateComponent(mLastTimeString+9, delta, kCentisecondsPerSecond);

	if (!delta) return;
	delta = UpdateComponent(mLastTimeString+6, delta, kSecondsPerMinute);

	if (!delta) return;
	delta = UpdateComponent(mLastTimeString+3, delta, kMinutesPerHour);

	if (!delta) return;
	delta = UpdateComponent(mLastTimeString+0, delta, kHoursPerDay);
}


void CalDayTime::TimeOfDay(char* time)
{
	UpdateTimeOfDay();
	memcpy(time, mLastTimeString, 12);
}

void CalDayTime::TimeOfDay(unsigned centisecondsOfDay, char* time)
{
	strcpy(time, "00:00:00.00");

	unsigned delta = centisecondsOfDay;

	if (!delta) return;
	delta = UpdateComponent(time+9, delta, kCentisecondsPerSecond);

	if (!delta) return;
	delta = UpdateComponent(time+6, delta, kSecondsPerMinute);

	if (!delta) return;
	delta = UpdateComponent(time+3, delta, kMinutesPerHour);

	if (!delta) return;
	delta = UpdateComponent(time+0, delta, kHoursPerDay);
}

unsigned CalDayTime::CentisecondsFromTime(const char* time)
{
	return ComponentToVal(time+0) * kCentisecondsPerHour
		+  ComponentToVal(time+3) * kCentisecondsPerMinute
		+  ComponentToVal(time+6) * kCentisecondsPerSecond
		+  ComponentToVal(time+9);
}
