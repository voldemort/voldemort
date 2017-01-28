if [ -f "leveldbfpic.patch" ]; then
	./make.sh
	export JAVA_LIBRARY_PATH="`pwd`/native/`uname -s`-`uname -p`/"
	export LD_LIBRARY_PATH=$JAVA_LIBRARY_PATH
else
	echo "this script should be run from its directory"
	echo ". ./setenv.sh"
fi

