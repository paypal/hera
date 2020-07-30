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
#include <errno.h>
#include <stdlib.h>

#include "utility/Assert.h"
#include "signal_manage.h"



/**
 * Set up a function to catch a signal
 * @param signal The signal you want to catch.
 * @param func The signal handler.
 * @return The old signal handler or SIG_ERR.
 */
sighandler_t sig_catch(int signal, sighandler_t func)
{
struct sigaction sa;
struct sigaction old_sa;

	sa.sa_handler = func;
	sa.sa_flags = 0;
	sigemptyset(&sa.sa_mask);
	if (sigaction(signal,&sa,&old_sa) == 0)
		return (sighandler_t )old_sa.sa_handler;
	else
		return SIG_ERR;
} // sig_catch()

/**
 * adds the specified signal to the list of signals to block
 * @param signal The signal to be blocked.
 * @param old_sigset (Optional) Pointer to a sigset for return value
 * @return 0 - success, -1 - failureThe old signal mask (sigset).
 */
int sig_block(int signal, sigset_t *old_sigset)
{
sigset_t ss;

	sigemptyset(&ss);
	sigaddset(&ss,signal);
	int rc = sigprocmask(SIG_BLOCK,&ss,old_sigset);
	return rc;
} // sig_block()

/**
 * remove the specified signal from the list of signals to block
 * @param signal Signal to be unblocked
 * @param old_sigset (Optional) Pointer to a sigset for return value
 * @return 0 - success, -1 - failureThe old signal mask (sigset).
 */
int sig_unblock(int signal, sigset_t *old_sigset)
{
sigset_t ss;

	sigemptyset(&ss);
	sigaddset(&ss,signal);
	int rc = sigprocmask(SIG_UNBLOCK,&ss,old_sigset);
	return rc;
} // sig_unblock()

/**
 * set the signal mask to the sigset provided
 * @param sigset Pointer to the new sigset.
 * @param old_sigset (Optional) Pointer to a sigset for return value
 * @return 0 - success, -1 - failureThe old signal mask (sigset).
 */
int sig_setmask(sigset_t *ss, sigset_t *old_sigset)
{
	int rc = sigprocmask(SIG_SETMASK,ss,old_sigset);
	return rc;
} // sig_setmask()

/**
 * set the mask to block NO signals
 * @param old_sigset (Optional) Pointer to a sigset for return value
 * @return 0 - success, -1 - failureThe old signal mask (sigset).
 */
int sig_blocknone(sigset_t *old_sigset)
{
sigset_t ss;

	sigemptyset(&ss);
	int rc = sigprocmask(SIG_SETMASK,&ss,old_sigset);
	return rc;
} // sig_blocknone()

/**
 * Pause the process until a signal occurs.
 * @param mask (Optional) Only signals in the mask will wake the process.
 */
void sig_pause(sigset_t *mask)
{
	sigset_t *ss = mask;

	if (ss == NULL)
	{
		ss = (sigset_t *)malloc(sizeof(sigset_t));
		sigemptyset(ss);
	}
	sigsuspend(ss);
	if (mask == NULL)
		free(ss);
} // sig_pause()


//-----------------------------------------------------------------------------


SigSet::SigSet()
{
	ASSERT(sigemptyset(&m_value) == 0);
}

SigSet &SigSet::add(int signal)
{
	ASSERT( sigaddset(&m_value, signal) == 0 );
	return *this;
}

SigSet &SigSet::del(int signal)
{
	ASSERT( sigdelset(&m_value, signal) == 0 );
	return *this;
}

SigSet &SigSet::empty()
{
	ASSERT( sigemptyset(&m_value) == 0 );
	return *this;
}

SigSet &SigSet::fill()
{
	ASSERT( sigfillset(&m_value) == 0 );
	return *this;
}

bool SigSet::is_member(int signal) const
{
	int rc = sigismember(&m_value, signal);
	ASSERT( rc != -1 );

	return (rc == 1);
}


//-----------------------------------------------------------------------------


SigBlocker::SigBlocker(const sigset_t &sigset) : m_suspend(false), m_unblocked(false)
{
	ASSERT( sigprocmask(SIG_BLOCK, &sigset, &m_old_sigset) == 0 );
}

SigBlocker::SigBlocker(const SigSet &sigset) : m_suspend(false), m_unblocked(false)
{
	ASSERT( sigprocmask(SIG_BLOCK, &(sigset.value()), &m_old_sigset) == 0 );
}

SigBlocker::~SigBlocker()
{
	if (!m_unblocked)
		unblock();
}

bool SigBlocker::unblock() // throw()
{
	if (m_suspend)
	{
		sigset_t zeromask;

		sigemptyset(&zeromask);
		// Unblock ALL signals and then pause for any signals
		sigsuspend(&zeromask);
	}

	if (sigprocmask(SIG_SETMASK, &m_old_sigset, NULL) == 0)
		m_unblocked = true;

	return m_unblocked;
}


//-----------------------------------------------------------------------------


SigCatcher::~SigCatcher()
{
	clear();
}

/**
 * @param signo The signal number we are looking for
 * @return A pointer to the first instance of a SigActionItem in the stack.
 *         NULL if no SigActionItem is found for the given signal number.
 */
SigCatcher::SigActionItem *SigCatcher::find(int signo)
{
	for (SigActionItem *ptr = m_action_stack; ptr != NULL; ptr = ptr->next)
	{
		if (ptr->signo == signo)
			return ptr;
	}

	return NULL;
}

/**
 * Multiple calls to set() on the same signal will install separate sigaction so that they
 * will unwind in reverse.
 *
 * @param signo The signal number we want to handle
 * @param handler The signal handler we want to install
 * @param flags (Optional) Sigaction flags we want to set for this signal. Defaults to 0.
 *        For example, you may want to set SA_RESTART so that system calls
 *        interrupted by the signal handler will be automatically restarted.
 * @param mask (Optional) The signal mask you want to set when the signal handler is invoked. Defaults to empty set.
 * @return SigCatcher::OK - All good
 *         SigCatcher::REGISTRATION_FAILED - sigaction call failed.
 */
SigCatcher::SIGCATCHER_RC SigCatcher::push(int signo, sighandler_t handler, int flags)
{
	SigSet empty;
	return push(signo, handler, flags, empty);
}

SigCatcher::SIGCATCHER_RC SigCatcher::push(int signo, sighandler_t handler, int flags, const SigSet &mask)
{
	SigActionItem *action_item = new SigActionItem(signo);

	struct sigaction action;

	action.sa_handler = handler;
	action.sa_mask = mask;
	action.sa_flags = flags;

	int rc = sigaction(signo, &(action), &(action_item->old_action));

	if (rc != 0)
	{
		delete action_item;
#ifdef USE_SIGNAL_EXCEPTION
		String msg("sigaction() failed");

		msg.append_formatted(": errno=%d", errno);
		throw SigCatcherError(msg);
#endif
		return PUSH_FAILED;
	}

	// place action_item at the top of the stack
	action_item->next = m_action_stack;

	if (m_action_stack)
		m_action_stack->prev = action_item;

	m_action_stack = action_item;

	return OK;
}

/**
 * This is restore the previous signal handler if one has been installed.
 * The first encountered SigActionItem will be used to restore the old
 * sigaction.
 */
SigCatcher::SIGCATCHER_RC SigCatcher::pop(int signo)
{
	SigActionItem *action_item = find(signo);

	if (action_item == NULL)
		return NO_HANDLER_FOUND;	// nothing to be done.

	int rc = sigaction(signo, &(action_item->old_action), NULL);

	// If sigaction errors out, there's not much we can do
	// still remove the SigActionItem.
	if (action_item->prev)
	{
		// middle of the list
		action_item->prev->next = action_item->next;
		action_item->next->prev = action_item->prev;
	}
	else
	{
		// top of the list
		m_action_stack = action_item->next;
		m_action_stack->prev = NULL;
	}

	delete action_item;

	if (rc)
	{
#ifdef USE_SIGNAL_EXCEPTION
		String msg("sigaction() failed");

		msg.append_formatted(": errno=%d", errno);
		throw SigCatcherError(msg);
#endif
		return POP_FAILED;
	}

	return OK;
}

SigCatcher::SIGCATCHER_RC SigCatcher::unset(int signo)
{
	SIGCATCHER_RC rc = OK;

	do
	{
		rc = pop(signo);
	} while (rc == OK);

	if (rc == NO_HANDLER_FOUND)
		return OK;

	return rc;
}

void SigCatcher::clear() // throw()
{
	SigActionItem *ptr = m_action_stack;

	while (ptr)
	{
		// Restore the old sigaction
		// we ignore return value here since there's nothing
		// we can do.
		sigaction(ptr->signo, &(ptr->old_action), NULL);

		SigActionItem *curr = ptr;
		ptr = ptr->next;

		delete curr;
	}
}
