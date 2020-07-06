#ifndef _OCCBIND_H_
#define _OCCBIND_H_

#include <oci.h>
#include "utility/Object.h"
#include "OCCGlobal.h"
#include <string>

struct column_output;

#define MAX_ARRAY_ROW_NUM 100

//-----------------------------------------------------------------------------

class OCCBind : public Object {
public:
	OCCBind();
	virtual ~OCCBind();

	std::string name;		//!< the name
	std::string value;		//!< the actual data of the bind
	OCILobLocator* lob;	//!< For LOB types
	OCIDateTime* date_time[MAX_ARRAY_ROW_NUM]; //!< For TIMESTAMP types
	OCIBind* bind;		//!< the oracle bind pointer
	occ::DataType type;	//!< Data type (BLOB, CLOB, etc)
	sb2 null_indicators[MAX_ARRAY_ROW_NUM];	//!< is the value NULL? (Not supported yet)

	unsigned int array_row_num;
	unsigned int array_max_data_size;
	ub2 bind_data_size[MAX_ARRAY_ROW_NUM];

	virtual bool is_inout(void) const { return is_inout_; }

protected:
	// Cannot use Polymorphism because this gets passed
	// around to OCI casted as (dvoid *)
	bool is_inout_;
};

class OCCBindInOut : public OCCBind
{
public:
	OCCBindInOut();
	virtual ~OCCBindInOut();

	unsigned int pos;	//!< Position in the RETURNING clause (0-based)
	ub4 maxlen;			//!< Maximum length for that out var
	ub4 rows;			//!< This stores the number of rows returned
	ub4 *lengths;		//!< This stores the actual lengths of the OUT placeholder (the array is all rows returned)
	ub2 *rcs;			//!< This stores return code from Oracle (the array is all rows returned)
	sb2 *indicators;	//!< NULL indicators

	// Data buffer
	char **buffer;			//!< Data buffer for non-lob

	// Borrowed pointers
	OCIError *errhp;

	void cleanup(void);
	int get_column(column_output *output, unsigned int pos);
	int get_oci_rc(unsigned int pos);
};

#endif
