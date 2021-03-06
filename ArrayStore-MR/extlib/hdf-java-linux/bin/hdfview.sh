#!/bin/sh

# Set up default variable values if not supplied by the user.

# where the HDFView is installed, e.g. /home/user1/hdfview
HDFVIEW_HOME=..
export HDFVIEW_HOME

# where Java is installed (requires jdk1.4.x or above), e.g. /usr/jdk1.4.2/bin
JAVAPATH=/usr/java/jdk-106/jdk/bin
export JAVAPATH

###############################################################################
#            DO NOT MODIFY BELOW THIS LINE
###############################################################################

CPATH=$HDFVIEW_HOME"/lib/jhdf.jar:"$HDFVIEW_HOME"/lib/jhdf5.jar:"$HDFVIEW_HOME"/lib/jhdfobj.jar"
CPATH=$CPATH":"$HDFVIEW_HOME"/lib/netcdf.jar:"$HDFVIEW_HOME"/lib/fits.jar:"$HDFVIEW_HOME"/lib/h5srb.jar"
CPATH=$CPATH":"$HDFVIEW_HOME"/lib/jhdf4obj.jar:"$HDFVIEW_HOME"/lib/jhdf5obj.jar:"$HDFVIEW_HOME"/lib/jhdfview.jar"
CPATH=$CPATH":"$HDFVIEW_HOME"/lib/jgraph.jar:"$HDFVIEW_HOME"/lib/ext/*"

TEST=/usr/bin/test
if [ ! -x /usr/bin/test ] 
then
TEST=`which test`
fi

if [ ! -d $JAVAPATH ]; then
    JAVALOC=`which java`
    LASTSLASH=`perl -e "print rindex(\"${JAVALOC}\", '/')"`
    JAVAPATH=`perl -e "print substr(\"${JAVALOC}\", 0, $LASTSLASH)"`
fi

if $TEST -z "$CLASSPATH"; then
	CLASSPATH=""
fi
CLASSPATH=$CPATH":"$CLASSPATH
export CLASSPATH

if $TEST -n "$JAVAPATH" ; then
	PATH=$JAVAPATH":"$PATH
	export PATH
fi


if $TEST -e /bin/uname; then
   os_name=`/bin/uname -s`
elif $TEST -e /usr/bin/uname; then
   os_name=`/usr/bin/uname -s`
else
   os_name=unknown
fi

if $TEST -z "$LD_LIBRARY_PATH" ; then
        LD_LIBRARY_PATH=""
fi

case  $os_name in
    SunOS)
	LD_LIBRARY_PATH=$HDFVIEW_HOME/lib/solaris:$HDFVIEW_HOME/lib/ext:$LD_LIBRARY_PATH
	;;
    Linux)
        LD_LIBRARY_PATH=$HDFVIEW_HOME"/lib/linux:"$HDFVIEW_HOME"/lib/ext:"$LD_LIBRARY_PATH
	;;
    IRIX*)
	OSREV=`/bin/uname -r`
	LD_LIBRARY_PATH=$HDFVIEW_HOME"/lib/irix-6.5:"$HDFVIEW_HOME"/lib/ext:"$LD_LIBRARY_PATH 
	LD_LIBRARYN32_PATH=$HDFVIEW_HOME"/lib/irix-6.5:"$HDFVIEW_HOME"/lib/ext":$LD_LIBRARY_PATH
	export LD_LIBRARYN32_PATH
	;;
    OSF1)
	LD_LIBRARY_PATH=$HDFVIEW_HOME"/lib/alpha:"$HDFVIEW_HOME"/lib/ext:"$LD_LIBRARY_PATH
	;;
    AIX)
	LD_LIBRARY_PATH=$HDFVIEW_HOME"/lib/aix:"$HDFVIEW_HOME"/lib/ext:"$LD_LIBRARY_PATH
	;;
    Darwin)
	DYLD_LIBRARY_PATH=$HDFVIEW_HOME"/lib/macosx:"$HDFVIEW_HOME"/lib/ext:"$DYLD_LIBRARY_PATH
	export DYLD_LIBRARY_PATH
	LD_LIBRARY_PATH=$DYLD_LIBRARY_PATH
	;;
    FreeBSD)
	LD_LIBRARY_PATH=$HDFVIEW_HOME"/lib/freebsd:"$HDFVIEW_HOME"/lib/ext:"$LD_LIBRARY_PATH
	;;
    *)
	echo "Unknown Operating System:  HDFView may not work correctly"
        ;;
esac

export LD_LIBRARY_PATH

$JAVAPATH/java -Xmx1000m -Djava.library.path=$LD_LIBRARY_PATH ncsa.hdf.view.HDFView -root $HDFVIEW_HOME $*
