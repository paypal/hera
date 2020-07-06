/*
 * fnv64a - 64 bit Fowler/Noll/Vo FNV-1a hash of a buffer or string
 *
 * @(#) $Revision: 1.1 $
 * @(#) $Id: fnv64a.c,v 1.1 2003/10/03 20:38:53 chongo Exp $
 * @(#) $Source: /usr/local/src/cmd/fnv/RCS/fnv64a.c,v $
 *
 ***
 *
 * usage:
 *	fnv1a64 [-b bcnt] [-m [-v]] [-s arg] [arg ...]
 *	fnv1a_64 [-b bcnt] [-m [-v]] [-s arg] [arg ...]
 *
 *	-b bcnt	  mask off all but the lower bcnt bits (default: 64)
 *	-m	  multiple hashes, one per line for each arg
 *	-s	  hash arg as a string (ignoring terminating NUL bytes)
 *	-v	  verbose mode, print arg after hash (implies -m)
 *	arg	  string (if -s was given) or filename (default stdin)
 *
 ***
 *
 * Fowler/Noll/Vo hash
 *
 * The basis of this hash algorithm was taken from an idea sent
 * as reviewer comments to the IEEE POSIX P1003.2 committee by:
 *
 *      Phong Vo (http://www.research.att.com/info/kpv/)
 *      Glenn Fowler (http://www.research.att.com/~gsf/)
 *
 * In a subsequent ballot round:
 *
 *      Landon Curt Noll (http://www.isthe.com/chongo/)
 *
 * improved on their algorithm.  Some people tried this hash
 * and found that it worked rather well.  In an EMail message
 * to Landon, they named it the ``Fowler/Noll/Vo'' or FNV hash.
 *
 * FNV hashes are designed to be fast while maintaining a low
 * collision rate. The FNV speed allows one to quickly hash lots
 * of data while maintaining a reasonable collision rate.  See:
 *
 *      http://www.isthe.com/chongo/tech/comp/fnv/index.html
 *
 * for more details as well as other forms of the FNV hash.
 *
 ***
 *
 * Please do not copyright this code.  This code is in the public domain.
 *
 * LANDON CURT NOLL DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO
 * EVENT SHALL LANDON CURT NOLL BE LIABLE FOR ANY SPECIAL, INDIRECT OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
 * USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *
 * By:
 *	chongo <Landon Curt Noll> /\oo/\
 *      http://www.isthe.com/chongo/
 *
 * Share and Enjoy!	:-)
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include "fnv.h"
//#include "longlong.h"

#define WIDTH 64	/* bit width of hash */

#define BUF_SIZE (32*1024)	/* number of bytes to hash at a time */

//pvoicu
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"

static char *usage = "usage: %s [-b bcnt] [-m [-v]] [-s arg] [arg ...]\n";
static char *program;	/* our name */


/*
 * print_fnv - print an FNV hash
 *
 * given:
 *	hval	  the hash value to print
 *	mask	  lower bit mask
 *	verbose	  1 => print arg with hash
 *	arg	  string or filename arg
 */
static void
print_fnv(Fnv64_t hval, Fnv64_t mask, int verbose, char *arg)
{
#if defined(HAVE_64BIT_LONG_LONG)
    if (verbose) {
	printf("0x%016llx %s\n", hval & mask, arg);
    } else {
	printf("0x%016llx\n", hval & mask);
    }
#else
    if (verbose) {
	printf("0x%08lx%08lx %s\n",
	       hval.w32[1] & mask.w32[1],
	       hval.w32[0] & mask.w32[0],
	       arg);
    } else {
	printf("0x%08lx%08lx\n",
	       hval.w32[1] & mask.w32[1],
	       hval.w32[0] & mask.w32[0]);
    }
#endif
}

// pvoicu - rename
/*
 * main - the main function
 *
 * See the above usage for details.
 */
int
main_fnv64a(int argc, char *argv[])
{
    char buf[BUF_SIZE+1];	/* read buffer */
    int readcnt;		/* number of characters written */
    Fnv64_t hval;		/* current hash value */
    int s_flag = 0;		/* 1 => -s was given, hash args as strings */
    int m_flag = 0;		/* 1 => print multiple hashes, one per arg */
    int v_flag = 0;		/* 1 => verbose hash print */
    int b_flag = WIDTH;		/* -b flag value */
    Fnv64_t bmask;		/* mask to apply to output */
    extern char *optarg;	/* option argument */
    extern int optind;		/* argv index of the next arg */
    int fd;			/* open file to process */
    int i;

    /*
     * parse args
     */
    program = argv[0];
    while ((i = getopt(argc, argv, "b:msv")) != -1) {
	switch (i) {
	case 'b':	/* bcnt bit mask count */
	    b_flag = atoi(optarg);
	    break;
	case 'm':	/* print multiple hashes, one per arg */
	    m_flag = 1;
	    break;
	case 's':	/* hash args as strings */
	    s_flag = 1;
	    break;
	case 'v':	/* verbose hash print */
	    m_flag = 1;
	    v_flag = 1;
	    break;
	default:
	    fprintf(stderr, usage, program);
	    exit(1);
	}
    }
    /* -s requires at least 1 arg */
    if (s_flag && optind >= argc) {
	fprintf(stderr, usage, program);
	exit(2);
    }
    /* limit -b values */
    if (b_flag < 0 || b_flag > WIDTH) {
	fprintf(stderr, "%s: -b bcnt: %d must be >= 0 and < %d\n",
		program, b_flag, WIDTH);
	exit(3);
    }
#if defined(HAVE_64BIT_LONG_LONG)
    if (b_flag == WIDTH) {
	bmask = (Fnv64_t)0xffffffffffffffffULL;
    } else {
	bmask = (Fnv64_t)((1ULL << b_flag) - 1ULL);
    }
#else
    if (b_flag == WIDTH) {
	bmask.w32[0] = 0xffffffffUL;
	bmask.w32[1] = 0xffffffffUL;
    } else if (b_flag >= WIDTH/2) {
	bmask.w32[0] = 0xffffffffUL;
	bmask.w32[1] = ((1UL << (b_flag-(WIDTH/2))) - 1UL);
    } else {
	bmask.w32[0] = ((1UL << b_flag) - 1UL);
	bmask.w32[1] = 0UL;
    }
#endif

    /*
     * start with the FNV-1a initial basis
     */
    hval = FNV1_64A_INIT;

    /*
     * string hashing
     */
    if (s_flag) {

	/* hash any other strings */
	for (i=optind; i < argc; ++i) {
	    hval = fnv_64a_str(argv[i], hval);
	    if (m_flag) {
		print_fnv(hval, bmask, v_flag, argv[i]);
	    }
	}


    /*
     * file hashing
     */
    } else {

	/*
	 * case: process only stdin
	 */
	if (optind >= argc) {

	    /* case: process only stdin */
	    while ((readcnt = read(0, buf, BUF_SIZE)) > 0) {
		hval = fnv_64a_buf(buf, readcnt, hval);
	    }
	    if (m_flag) {
		print_fnv(hval, bmask, v_flag, "(stdin)");
	    }

	} else {

	    /*
	     * process any other files
	     */
	    for (i=optind; i < argc; ++i) {

		/* open the file */
		fd = open(argv[i], O_RDONLY);
		if (fd < 0) {
		    fprintf(stderr, "%s: unable to open file: %s\n",
			    program, argv[i]);
		    exit(4);
		}

		/*  hash the file */
		while ((readcnt = read(fd, buf, BUF_SIZE)) > 0) {
		    hval = fnv_64a_buf(buf, readcnt, hval);
		}

		/* finish processing the file */
		if (m_flag) {
		    print_fnv(hval, bmask, v_flag, argv[i]);
		}
		close(fd);
	    }
	}
    }

    /*
     * report hash and exit
     */
    if (!m_flag) {
	print_fnv(hval, bmask, v_flag, "");
    }
    return 0;	/* exit(0); */
}

//pvoicu
#pragma GCC diagnostic pop
