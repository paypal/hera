SQL driver for occ, implementing basic operations as well as basic types (int, string).

Currently only the driver using SSL configured from the protecteds is implemented at "gomuxdriver/occopenssl", do import _ "gomuxdriver/occopenssl" to use it.

It depends on Infra-R/utility for the logger and Netstring encoder/decoder. gooccdriver/occopenssl also depends on Infra-R/spacemonkeyopenssl
