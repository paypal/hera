#ifndef _COLUMNINFO_H_
#define _COLUMNINFO_H_

#include <string>
#include <oci.h>

struct ColumnInfo
{
	std::string name;
	ub2	   type;
	ub2    width;
	ub1    precision;
	ub1    scale;

	ColumnInfo();
	ColumnInfo(const ColumnInfo& _col);
	ColumnInfo& operator = (const ColumnInfo& _col);

private:
	void copy(const ColumnInfo& _col);
};

#endif
