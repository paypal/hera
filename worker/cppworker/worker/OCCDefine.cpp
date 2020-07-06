#include <memory.h>
#include <stdio.h>

#include "OCCDefine.h"
#include <assert.h>

//-------------------------------------------------------------------------------------------------

OCCDefine::OCCDefine()
{
	data = NULL;
	indicator = NULL;
	str_size = NULL;
	define = NULL;
	lob = NULL;
	datetime = 0;
	rows = 0;
	column_size = 0;
	initialized = 0;
	own_data = true;
	own_indicator = true;
	own_str_size = true;
}

//-------------------------------------------------------------------------------------------------

OCCDefine::~OCCDefine()
{
	int i;

	if (own_data)
		delete[] data;
	if (own_indicator)
		delete[] indicator;
	if (own_str_size)
		delete[] str_size;
	if (lob)
	{
		for(i=0;i<rows;i++)
			OCIDescriptorFree((dvoid*)(lob[i]),OCI_DTYPE_LOB);
		delete[] lob;
	}
	if (datetime)
	{
		for(i=0;i<rows;i++)
			OCIDescriptorFree((dvoid*)(datetime[i]), (type == SQLT_TIMESTAMP) ? OCI_DTYPE_TIMESTAMP : OCI_DTYPE_TIMESTAMP_TZ);
		delete[] datetime;
	}
}

//-------------------------------------------------------------------------------------------------

int OCCDefine::init(int _rows, int _column_size, ub2 _type, OCIEnv* envhp, char *data_buf, sb2 *indicator_buf, ub2* str_size_buf)
{
	int i;
	
	oci_rc = 0;

	if(initialized)
		return -1;
	
	if(_type != SQLT_STR && _type != SQLT_BLOB && _type != SQLT_CLOB && _type != SQLT_LNG && _type != SQLT_BIN && _type!= SQLT_TIMESTAMP && _type!= SQLT_TIMESTAMP_TZ)
		return -1;
	
	rows = _rows;
	type = _type;
	column_size = _column_size;

	indicator = (indicator_buf != NULL) ? indicator_buf : new sb2[rows];
	own_indicator = (indicator_buf == NULL);

	if ((type == SQLT_STR) || (type == SQLT_LNG) || (type == SQLT_BIN))
	{
		data = (data_buf != NULL) ? data_buf : new char[rows * column_size];
		own_data = (data_buf == NULL);
		str_size = (str_size_buf != NULL) ? str_size_buf : new ub2[rows];
		own_str_size = (str_size_buf == NULL);
	} else if ((type == SQLT_TIMESTAMP) || (type == SQLT_TIMESTAMP_TZ)) {
		datetime = new OCIDateTime*[rows];
		memset(datetime, 0, rows * sizeof(OCIDateTime *));
		for (i = 0; i < rows; i++)
		{
			oci_rc = OCIDescriptorAlloc((dvoid*) envhp, (dvoid**) &(datetime[i]), (type == SQLT_TIMESTAMP) ? OCI_DTYPE_TIMESTAMP : OCI_DTYPE_TIMESTAMP_TZ, 0, NULL);
			if (oci_rc != OCI_SUCCESS)
				return -1;
		}
	} else
	{
		// type==SQLT_BLOB || type==SQLT_CLOB
		lob = new OCILobLocator*[rows];
		memset(lob, 0, rows * sizeof(OCILobLocator *));
		for (i = 0; i < rows; i++)
		{
			oci_rc = OCIDescriptorAlloc((dvoid*) envhp, (dvoid**) &(lob[i]), OCI_DTYPE_LOB, 0, NULL);
			if (oci_rc != OCI_SUCCESS)
				return -1;
		}
	}

	initialized = 1;
	return 0;
}

//-------------------------------------------------------------------------------------------------

int OCCDefine::clear_indicators()
{
	oci_rc = 0;
	
	if(!initialized)
		return -1;
	
	memset(indicator, 0, (sizeof(sb2) * rows));

	return 0;
}

//-------------------------------------------------------------------------------------------------

int OCCDefine::define_by_pos(OCIStmt* stmthp, OCIError* errhp, int column_pos, 
														ub4 oracle_lobprefetch_size)
{
	oci_rc = 0;
	
	if(!initialized)
		return -1;
	
	define = NULL;
	
	if(type==SQLT_STR || type==SQLT_LNG) {
		if((oci_rc = OCIDefineByPos(stmthp, &define, errhp, column_pos, data,
		 (sword)column_size, SQLT_STR, indicator, str_size, (ub2*)NULL, OCI_DEFAULT)))
			return -1;
	} else if (type==SQLT_BIN){
		if((oci_rc = OCIDefineByPos(stmthp, &define, errhp, column_pos, data,
		 (sword)column_size, SQLT_LVB, indicator, (ub2*)NULL, (ub2*)NULL, OCI_DEFAULT)))
			return -1;
	} else if (type==SQLT_TIMESTAMP){
		if((oci_rc = OCIDefineByPos(stmthp, &define, errhp, column_pos, (void*)datetime,
		 sizeof(OCIDateTime*), SQLT_TIMESTAMP, indicator, (ub2*)NULL, (ub2*)NULL, OCI_DEFAULT)))
			return -1;
	} else if (type==SQLT_TIMESTAMP_TZ){
		if((oci_rc = OCIDefineByPos(stmthp, &define, errhp, column_pos, (void*)datetime,
		 sizeof(OCIDateTime*), SQLT_TIMESTAMP_TZ, indicator, (ub2*)NULL, (ub2*)NULL, OCI_DEFAULT)))
			return -1;
	} else {
		// type==SQLT_BLOB || type==SQLT_CLOB
		if((oci_rc = OCIDefineByPos(stmthp, &define, errhp, column_pos, (dvoid*)lob,
						-1, type, indicator, (ub2*)NULL, (ub2*)NULL, OCI_DEFAULT)))
			return -1;

		boolean enable_prefetch_length = true;
		if ((oci_rc= OCIAttrSet((dvoid *)define, OCI_HTYPE_DEFINE,(dvoid *) &enable_prefetch_length, 
						0, OCI_ATTR_LOBPREFETCH_LENGTH, errhp))) 
			return -2;  // -2 means failure but continue

		if (oracle_lobprefetch_size>0) { 
			if ((oci_rc = OCIAttrSet ((dvoid *)define,  OCI_HTYPE_DEFINE, (void *) &oracle_lobprefetch_size,
							0, OCI_ATTR_LOBPREFETCH_SIZE, errhp)))
				return -2;  // -2 means failure but continue
		}
	}

	return 0;
}

//-------------------------------------------------------------------------------------------------

int OCCDefine::get_column(int i, column_output* output)
{
	oci_rc = 0;
	
	if(!initialized)
		return -1;

	if(i>=rows)
		return -1;

	output->type = type;
	output->column_size = column_size;
	if(type==SQLT_STR || type == SQLT_LNG || type == SQLT_BIN) {
		output->data = data + i*column_size;
		output->str_size = str_size[i];
		output->data[column_size - 1] = 0; // null terminate to be safe
	} else
		output->data = NULL;
	output->indicator = indicator[i];

	output->lob = NULL;
	output->datetime = NULL;
	if(type==SQLT_BLOB || type==SQLT_CLOB)
		output->lob = lob[i];
	else if ((type==SQLT_TIMESTAMP) || (type==SQLT_TIMESTAMP_TZ))
		output->datetime = datetime[i];

	return 0;
}

//-------------------------------------------------------------------------------------------------

int OCCDefine::get_oci_rc()
{
	return oci_rc;
}
