#!/bin/bash

# rebuilds are for developing this script
if [ x$1 = xrebuild ]
then
    find . -name 'vet.log*' | xargs /bin/rm
fi

out=0
# find directories with go source files
for e in `find . -name '*.go' |sed -e 's,/[^/]*$,,'|sort -u`
do 
    (
    d=`echo $e | sed -e 's,^..,,'`
    cd $d
    go vet -c=3 github.com/paypal/hera/$d 2> vet.log3 
    rv=$?
    if [ $rv -ne 0 ]
    then
        # remove line numbers
        sed -e 's/^[0-9]*//;s/[.]go:[0-9]*:[0-9]*: /.go /' vet.log3 > vet.log2

        # save the go vet output if we are rebuilding
        if [ x$1 = xrebuild -a ! -e vet.log ]
        then
            cp vet.log{2,}
        fi

        # diff go vet output against dev commit vet.log
        if [ ! -e vet.log ]
        then
            cat vet.log2
            rvd=2
        else
            diff vet.log{,2}
            rvd=$?
        fi
        if [ $rvd -ne 0 ]
        then
            echo $rvd rv-vet-diff $d
            exit $rvd
        fi
    fi 
    ) 
    subRv=$?
    if [ $subRv -ne 0 ]
    then
        out=$subRv
    fi
done

if [ x$1 = xrebuild ]
then
    time ./govet.sh
    out=$?
    echo `date` "second pass complete for rebuild"
fi

echo $out overall go vet rv
exit $out
