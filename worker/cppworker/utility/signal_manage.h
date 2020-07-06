#ifndef _SIGNAL_MANAGE_H_
#define _SIGNAL_MANAGE_H_

/*
  This is a suite of C routines to facilitate simple signal management

  uses the POSIX sigaction type routines for reliable operation

  based on code by djb

  04/30/98 - ech - created
*/

#include <signal.h>
#include <stddef.h>

#ifdef SOLARIS_PORT
typedef void(*sighandler_t)(int);
#endif

#ifdef USE_SIGNAL_EXCEPTION
#include <infra/utility/core/lang/PPException.h>
#endif


/**
 * @class SigSet
 * This class is used to build the signal mask. The idea is to use call chaining
 * to build the sigset value.
 * e.g. SignalSet(SIGINT).add(SIGTERM).add(SIGALRM)
 */
class SigSet
{
private:
	sigset_t m_value;

public:
	SigSet();

	SigSet& add(int signal);
	SigSet& del(int signal);
	SigSet& empty(void);
	SigSet& fill(void);
	bool is_member(int signal) const;
	operator sigset_t() const { return m_value; }
	const sigset_t& value(void) const { return m_value; }

private:
	// These are not allowed
	// (presumably because there is no "sigcopyset()" function that allows us to copy a sigset_t)
	SigSet(const SigSet& set);
	SigSet& operator=(const SigSet& set);
	SigSet& operator=(sigset_t mask);
};


/**
 * @class SigBlocker
 * This class provide a scoped signal blocking functionality.
 * Typically, you'd create an instance of this class on the stack
 * to block signal for a critical section. On cleanup, the old
 * signal mask will be restored.
 *
 * NOTE: On construction, this class calls sigprocmask with
 * SIG_BLOCK. That means SigBlocker works cumulatively. If you
 * have more than one SigBlocker active, the signals that are
 * blocked are the union of them all.
 * When a SigBlocker is destroyed it restores the signal mask
 * before it's constructed. That means it should be ok to nest
 * these guys.
 * Example:
 *
 * {
 *   // Assuming we don't have any signal mask set before this
 *
 *   SigBlocker block_sigchld(SigSet().add(SIGCHILD));
 *   // Only SIGCHLD is blocked
 *   {
 *     SigBlocker block_alarm(SigSet().add(SIGALRM));
 *     // Now SIGCHLD & SIGALRM are blocked
 *     ...
 *   }
 *   // Now only SIGCHLD is blocked
 * }
 * // All signals unblocked.
 */
class SigBlocker
{
private:
	sigset_t m_old_sigset;
	bool m_suspend;
	bool m_unblocked;

public:
	explicit SigBlocker(const sigset_t &sigset);
	explicit SigBlocker(const SigSet &sigset);
	~SigBlocker();

	//!< @brief Set suspend to pause on destruction, waiting for any signal
	void set_suspend(void) { m_suspend = true; }
	bool unblock(void);
};


/**
 * Use this class to manage signal handlers.
 * It provides a bit more sophisticated behaviour than sig_catch.
 * For each signal handler registered with SigCatcher, SigCatcher
 * will restore the previous sigaction at destruction/unset.
 * Signal handlers are added in order like a stack and are unwound
 * in a Last-In-First-Out manner.
 * For example:
 *
 * {
 *   SigCatcher sighandler;
 * 
 *   sighandler.push(SIGALRM, alrm_handler1);
 *   // alrm_handler1 is the signal handler for SIGALRM
 *   ...
 *   {
 *     sighandler.push(SIGALRM, alrm_handler2);
 *     // Now alrm_handler2 is the signal handler for SIGALRM
 *     ...
 *     sighandler.pop(SIGALRM);
 *     // Now alrm_handler2 is the signal handler
 *   }
 * }
 * // SIG_DFL is the handler for SIGALRM
 *
 */
class SigCatcher
{
private:
	struct SigActionItem
	{
		int signo;
		struct sigaction old_action;
		SigActionItem *next;
		SigActionItem *prev;

		SigActionItem(int sig) : signo(sig), next(NULL), prev(NULL) {}
		~SigActionItem() {}
	};

	SigActionItem *m_action_stack;

public:
#ifdef USE_SIGNAL_EXCEPTION
	class SigCatcherError : public PPException
	{
	public:
		SigCatcherError(const String &msg) : PPException(msg) {}
		~SigCatcherError() {}

		String get_name(void) const { return "SigCatcherError"; }
	};
#endif

	enum SIGCATCHER_RC
	{
		OK = 0,
		PUSH_FAILED = -1,
		POP_FAILED = -2,
		NO_HANDLER_FOUND = -3,
		INTERNAL_ERROR = -4,
	};

	SigCatcher() : m_action_stack(NULL) {}
	~SigCatcher();

	//!< @brief Call this method repeatedly to register new signal handlers
	SIGCATCHER_RC push(int signo, sighandler_t handler, int flags = 0);
	SIGCATCHER_RC push(int signo, sighandler_t handler, int flags, const SigSet& mask);
	//!< @brief Call this method to remove the topmost signal handler for the given signal number
	SIGCATCHER_RC pop(int signo);
	//!< @brief Clear all signal handlers for the given signal number.
	SIGCATCHER_RC unset(int signo);
	//!< @brief Clear all signal handlers.
	void clear(void);

private:
	//!< @brief find the first instance of a signal handler for the given signal number.
	SigActionItem *find(int signo);

	// These are not allowed
	SigCatcher(const SigCatcher &src);
	SigCatcher &operator=(const SigCatcher &src);
};


#ifdef __cplusplus
extern  "C" {
#endif

//Set up a function to catch a signal
sighandler_t sig_catch(int signal, sighandler_t func);
//adds the specified signal to the list of signals to block
int sig_block(int signal, sigset_t *old_sigset=NULL);
//remove the specified signal from the list of signals to block
int sig_unblock(int signal, sigset_t *old_sigset=NULL);
//set the signal mask
int sig_setmask(sigset_t *ss, sigset_t *old_sigset=NULL);
//set the mask to block NO signals
int sig_blocknone(sigset_t *old_sigset=NULL);
//pause the current process for signals
void sig_pause(sigset_t *mask=NULL);

#ifdef __cplusplus
}
#endif


#endif //_SIGNAL_MANAGE_H_
