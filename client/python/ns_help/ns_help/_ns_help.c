#include <Python.h>
#include <pymem.h>
#include <string.h>
#ifdef TEST_CHRIS
#include <stdio.h>
#include <sys/types.h>
#endif



/* Errors that can occur during netstring parsing */
#define NETSTRING_ERROR_TOO_LONG     -1
#define NETSTRING_ERROR_NO_COLON     -2
#define NETSTRING_ERROR_TOO_SHORT    -3
#define NETSTRING_ERROR_NO_COMMA     -4
#define NETSTRING_ERROR_LEADING_ZERO -5
#define NETSTRING_ERROR_NO_LENGTH    -6
#define NETSTRING_ERROR_CODE_WRONG   -7

char * errs[] = {"NA", "String longer than 999999999 bytes",
                 "String missing colon",
                 "String shorter than length advertised",
                 "String missing comma",
                 "String beginning with zero",
                 "String without length prefix",
                 "String with malformed payload code",
                 NULL};

/* python2 / python3 stuff
 */

/* Streaming API for netstrings. */


int netstring_read(char *buffer, size_t buffer_length,
                   char **netstring_start, ssize_t *netstring_length,
                   unsigned long long * out_code) {
    size_t i;
    size_t j;
    size_t len = 0;
    unsigned long long code = 0;
    int space_adjustment = 0;

    /* Write default values for outputs */
    *netstring_start = NULL; 
    *netstring_length = 0;

    /* Make sure buffer is big enough. Minimum size is 3. */
    if (buffer_length < 3) return NETSTRING_ERROR_TOO_SHORT;

    /* No leading zeros allowed! */
    if (buffer[0] == '0' && isdigit(buffer[1])) {
        return NETSTRING_ERROR_LEADING_ZERO;
    }

    /* The netstring must start with a number */
    if (!isdigit(buffer[0])) return NETSTRING_ERROR_NO_LENGTH;

    /* Read the number of bytes */
    for (i = 0; i < buffer_length && isdigit(buffer[i]); i++) {
        /* Error if more than 9 digits */
        if (i >= 9) return NETSTRING_ERROR_TOO_LONG;
        /* Accumulate each digit, assuming ASCII. */
        len = len*10 + (buffer[i] - '0');
    }

    /* Check buffer length once and for all. Specifically, we make sure
     that the buffer is longer than the number we've read, the length
     of the string itself, and the colon and comma. */
    if (i + len + 1 >= buffer_length) return NETSTRING_ERROR_TOO_SHORT;

    /* Read the colon */
    if (buffer[i++] != ':') return NETSTRING_ERROR_NO_COLON;
  
    /* Test for the trailing comma, and set the return values */
    if (buffer[i + len] != ',') return NETSTRING_ERROR_NO_COMMA;

    /* special extra here:  assume each buffer starts like '[digits]+\ .*" */
    code = 0;
    for (j = i; j < buffer_length && (buffer[j] != ' ') && (buffer[j] != ','); j++) {
        if (j > buffer_length) {
            break;
        }
        if (!isdigit(buffer[j])) {
            return NETSTRING_ERROR_CODE_WRONG;
        }
        code = code * 10 + (buffer[j] -'0');  /* this will overflow */
    }
    *out_code = code;
    if (buffer[j] == ',') {
        *netstring_start = 0;
        *netstring_length = 0;
        space_adjustment = -1;
    }
    if (buffer[j] == ' ') { 
        *netstring_start = &buffer[j + 1]; 
        *netstring_length = len - j - 1 + i;
    } 
    return i + 2 /* length */ + *netstring_length /* data */ + 1 /* for the comma */ + space_adjustment /* for OCC nulls */;
}


PyObject *
py_getnetstrings(PyObject * self,
                 PyObject * args) {
    PyObject *         result = NULL;
    char *             in_string = NULL;
    ssize_t            in_string_len = 0;
    ssize_t            bytes_read = 0;
    unsigned long long code = 0;
    char *             payload = NULL;
    ssize_t            payload_len = 0;
    PyObject *         temp_tuple = NULL;
    
    if (PyArg_ParseTuple(args, "s#", & in_string, & in_string_len)) {
        result = PyList_New(0);
        if (in_string_len > 999999999) {  /* per DJB */
            PyErr_SetString(PyExc_ValueError, errs[1]);
            return NULL;
        }
        while (in_string_len >= 3) {
            bytes_read = netstring_read(in_string, in_string_len,
                                        & payload, & payload_len, & code);
            if (bytes_read < 0) {
                PyErr_SetString(PyExc_ValueError, errs[-bytes_read]);
                return NULL;
            }
            temp_tuple = PyTuple_New(2);
            PyTuple_SetItem(temp_tuple, 0,
                            PyLong_FromLongLong(code));
            PyTuple_SetItem(temp_tuple, 1,
                            PyBytes_FromStringAndSize(payload, 
                                                      payload_len));  /* should check for fail*/
            PyList_Append(result, 
                          temp_tuple);
            in_string_len -= bytes_read;
            in_string += bytes_read;
        }
                          
    } else {
        result = Py_None;
        Py_INCREF(result);
    }
    return result;
}


static PyMethodDef ns_help_methods[] = { 
    {"get_netstrings", (PyCFunction)py_getnetstrings, METH_VARARGS, 
     "get netstrings as list of tuples from sting"},
    {NULL} };


#if PY_MAJOR_VERSION >= 3

static int ns_help_traverse(PyObject *m, visitproc visit, void *arg) {
    return 0;
}

static int ns_help_clear(PyObject *m) {
    return 0;
}

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_ns_help",
        NULL,
        0,
        ns_help_methods,
        NULL,
        ns_help_traverse,
        ns_help_clear,
        NULL
};

#define INITERROR return NULL

PyMODINIT_FUNC
PyInit_ns_help(void)
#else

#define INITERROR return
PyMODINIT_FUNC init_ns_help(void)
#endif 
{
    PyObject *module;
#if PY_MAJOR_VERSION >=3
    module = PyModule_Create(&moduledef);
#else
    module = Py_InitModule3("_ns_help", ns_help_methods, "fast netstrings");
#endif

    if (module == NULL) {
      INITERROR;
    }
#if PY_MAJOR_VERSION >=3
    return module;
#endif
}

#ifdef TEST_CHRIS

char * test = "8:3 118040,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284532945,6:3 1182,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100914_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284447600,8:3 118040,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284532945,6:3 1182,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100914_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284447600,8:3 118047,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284539513,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284539513,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 H,29:3 GenericLoader file loading.,12:3 1284539540,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 H,36:3 Meta data updated by FPSM manager.,12:3 1284539541,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 F,29:3 GenericLoader file loading.,12:3 1284539541,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 T,42:3 Processed by platform service framework.,12:3 1284539523,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118047,3:3 E,42:3 Processed by platform service framework.,12:3 1284539523,6:3 1206,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100915_star_bin_file_3_test.txt,1:3,3:3 3,12:3 1284534000,8:3 118050,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284540093,6:3 1222,1:3,1:3,1:3,3:3 ?,5:3 426,44:3 p20100915_star_ewallet_bin_file_4_test.txt,1:3,3:3 0,12:3 1284534000,8:3 118050,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284540093,6:3 1222,1:3,1:3,1:3,3:3 ?,5:3 426,44:3 p20100915_star_ewallet_bin_file_4_test.txt,1:3,3:3 0,12:3 1284534000,8:3 118050,3:3 E,6:3 test,12:3 1284540188,6:3 1222,1:3,1:3,1:3,3:3 ?,5:3 426,44:3 p20100915_star_ewallet_bin_file_4_test.txt,1:3,3:3 0,12:3 1284534000,8:3 118051,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1284720562,6:3 1229,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100917_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284706800,8:3 118051,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1284720563,6:3 1229,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100917_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284706800,8:3 118051,3:3 M,6:3 test,12:3 1284720783,6:3 1229,1:3,1:3,1:3,3:3 ?,5:3 422,36:3 p20100917_star_bin_file_1_test.txt,1:3,3:3 0,12:3 1284706800,8:3 118422,3:3 L,60:3 Inbound file registered as expected by scripting interface,12:3 1308962363,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 L,75:3 FPSM Manager updates Filename and External Filename with advanced tokens.,12:3 1308962363,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 E,61:3 Inbound file registered as received by scripting interface.,12:3 1308962380,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 H,29:3 GenericLoader file loading.,12:3 1308962425,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,8:3 118422,3:3 H,21:3 Update record count,12:3 1308962426,6:3 2559,1:3,1:3,1:3,3:3 F,1:3,26:3 20110624_eft_input_2.txt,1:3,3:3 1,12:3 1308962331,";

int 
main(int     argc,
     char ** argv) {
    
    ssize_t in_string_len = strlen(test);
    char * in_string = test;
    ssize_t            bytes_read = 0;
    unsigned long long code = 0;
    char *             payload = NULL;
    ssize_t            payload_len = 0;

    while (in_string_len >=3) {

        bytes_read = netstring_read(in_string, in_string_len, 
                                    & payload, &payload_len, & code);
        if (bytes_read < 0) {
            printf("Error %ld\n", (long int)errs[-bytes_read]);
            return -1;
        }
        printf("Read %ld bytes, got code %lld data {%.*s}\n", bytes_read,
               code, (int)(payload_len & 0xffffffff), payload);
        in_string_len -= bytes_read; 
        in_string += bytes_read;
    }
    printf("Done\n");
}

#endif 
