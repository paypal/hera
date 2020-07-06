#include "ColumnInfo.h"

ColumnInfo::ColumnInfo()
{

}

ColumnInfo::ColumnInfo(const ColumnInfo& _col):
			name(_col.name),
			type(_col.type),
			width(_col.width),
			precision(_col.precision),
			scale(_col.scale)
{
}

ColumnInfo& ColumnInfo::operator = (const ColumnInfo& _col)
{
	copy(_col);
	return *this;
}

void ColumnInfo::copy(const ColumnInfo& _col)
{
	name = _col.name;
	type = _col.type;
	width = _col.width;
	precision = _col.precision;
	scale = _col.scale;
}
