#include "OCCBind.h"
#include "OCCDefine.h"

#include <string>
#include <string.h>

//-----------------------------------------------------------------------------

OCCBind::OCCBind() : name(), 
                     value(), 
                     lob(NULL), 
                     bind(NULL), 
                     type(occ::OCC_TYPE_STRING),
                     array_row_num(0),
                     array_max_data_size(0),
                     is_inout_(false)
{
	memset(null_indicators, 0, sizeof(null_indicators));
	date_time[0]=NULL;
}

//-----------------------------------------------------------------------------

OCCBind::~OCCBind()
{
	if(lob) {
		OCIDescriptorFree((dvoid*)lob, OCI_DTYPE_LOB);
	}
	if (date_time[0] != NULL) {
		OCIArrayDescriptorFree((dvoid**)date_time, (type == occ::OCC_TYPE_TIMESTAMP) ? OCI_DTYPE_TIMESTAMP : OCI_DTYPE_TIMESTAMP_TZ);
	}

	// We are not explicitly freeing bind handles. They'll be freed when
	// their parent statement  handles are freed.
}

OCCBindInOut::OCCBindInOut() : pos(0),
                               maxlen(0),
                               rows(0),
                               lengths(NULL),
                               rcs(NULL),
							   indicators(NULL),
                               buffer(NULL),
							   errhp(NULL)
{
	is_inout_ = true;
}

OCCBindInOut::~OCCBindInOut()
{
	cleanup();
}

void OCCBindInOut::cleanup()
{
	if (buffer == NULL)
		return;

	for (ub4 i = 0; i < rows; ++i)
		delete[] buffer[i];

	delete[] buffer;
	buffer = NULL;
	delete[] lengths;
	lengths = NULL;
	delete[] rcs;
	rcs = NULL;
	delete[] indicators;
	indicators = NULL;
}

int OCCBindInOut::get_column(column_output *output, unsigned int row)
{
	if (buffer == NULL)
		return -1;

	if (row >= rows)
		return -1;

	output->column_size = lengths[row];
	output->data = buffer[row];
	output->data[lengths[row]] = '\0';
	output->indicator = indicators[row];

	return 0;
}

int OCCBindInOut::get_oci_rc(unsigned int row)
{
	if (row >= rows)
		return -1;

	return rcs[row];
}
