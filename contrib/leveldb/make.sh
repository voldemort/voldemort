
if [ "$JAVA_HOME" = "" ]; then
	echo "you must define JAVA_HOME"
	exit 0
fi

if [ "`which git`" = "" ]; then
	echo "you must install git"
	exit 0
fi

if [ ! -d "./leveldb" ]; then
	git clone https://code.google.com/p/leveldb/
fi

if [ ! -d "./snappy-1.0.4" ]; then

	if [ "`which wget`" = "" ]; then
		echo "please download snappy-1.0.4 manually or install wget"
		exit 0
	fi
	wget http://snappy.googlecode.com/files/snappy-1.0.4.tar.gz

 	if [ -f ./snappy-1.0.4.tar.gz ]; then
 		tar -xzf ./snappy-1.0.4.tar.gz

 		if [ -d "./snappy-1.0.4" ]; then
			rm ./snappy-1.0.4.tar.gz
		else
			echo "please check if snappy unzip properly"
			exit 0
		fi
	else 
		echo "please download snappy-1.0.4 manually"
		exit 0
 	fi
fi

cd snappy*                                    
if [ ! -f .libs/libsnappy.a ]; then        
  make clean   
  ./configure --disable-shared --with-pic    
  make                                        
fi                                            
cd ../leveldb                                 
git checkout c8c5866a86c8
export LIBRARY_PATH=`cd ../snappy-1.0.4; pwd` 
export LIBRARY_PATH=`cd ../snappy-1.0.4/.libs; pwd`:$LIBRARY_PATH 
export C_INCLUDE_PATH=${LIBRARY_PATH}         
export CPLUS_INCLUDE_PATH=${LIBRARY_PATH}     
patch -stN -p 0 < ../leveldbfpic.patch
make
export LIBRARY_PATH=`pwd`:$LIBRARY_PATH                                      
export C_INCLUDE_PATH=C_INCLUDE_PATH:`pwd`/include     
export CPLUS_INCLUDE_PATH=CPLUS_INCLUDE_PATH:`pwd`/include     
cd ..                                         

make
