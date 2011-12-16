#!/bin/sh

# Delete existing build_config.mk
rm -f build_config.mk

# Detect OS
case `uname -s` in
    Darwin)
        PLATFORM=OS_MACOSX
        echo "PLATFORM_CFLAGS=-DOS_MACOSX -dynamiclib" >> build_config.mk
        echo "PLATFORM_LDFLAGS=-dynamiclib -framework JavaVM -lsnappy -lleveldb"  >> build_config.mk
        echo "PLATFORM_JNIEXT=jnilib" >> build_config.mk
        ;;
    Linux)
        PLATFORM=OS_LINUX
        echo "PLATFORM_CFLAGS=-pthread -fPIC -DOS_LINUX -I\$\$JAVA_HOME/include/linux" >> build_config.mk
        echo "PLATFORM_LDFLAGS=-lpthread -shared -Wl,--whole-archive -lleveldb -lsnappy -Wl,--no-whole-archive -lc -Wl,-soname -Wl,libldb.so"  >> build_config.mk
        echo "PLATFORM_JNIEXT=so" >> build_config.mk
        ;;
    *)
        echo "Unknown platform!"
        exit 1
esac

echo "PLATFORM=`uname -s`-`uname -p`" >> build_config.mk

mkdir -p native/`uname -s`-`uname -p`

# Keep leveldb's flags
# On GCC, use libc's memcmp, not GCC's memcmp
PORT_CFLAGS="-fno-builtin-memcmp"

echo "PORT_CFLAGS=$PORT_CFLAGS" >> build_config.mk
