# Copyright 2022 PayPal Inc
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

def main():
	""" Converts key=value file to cdbmake format

	python cdbmake.py input.txt | cdbmake out.cdb out.cdb.tmp
	"""

	with open(sys.argv[1]) as fh:
		for l in fh:
			# input  ---- one = Hello
			# output ---- +3,5:one->Hello
			(k,nil,v) = l.partition('=')
			k=k.strip()
			v=v.strip()
			sys.stdout.write("+%d,%d:%s->%s\n"%(len(k),len(v),k,v))
	sys.stdout.write("\n") # blank line to end

if __name__ == "__main__":
	main()
