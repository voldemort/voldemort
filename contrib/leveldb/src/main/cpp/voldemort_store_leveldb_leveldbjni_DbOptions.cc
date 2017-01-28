#include <jni.h> 
#include <iostream>
#include "voldemort_store_leveldb_leveldbjni_DbOptions.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include <string>
#include "leveldb/cache.h"

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setCreateIfMissing
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setCreateIfMissing
  (JNIEnv * env, jclass c, jlong h, jboolean v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->create_if_missing = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setErrorIfExists
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setErrorIfExists
  (JNIEnv * env, jclass c, jlong h, jboolean v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->error_if_exists = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setParanoidChecks
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setParanoidChecks
  (JNIEnv * env, jclass c, jlong h, jboolean v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->paranoid_checks = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setWriteBufferSize
 * Signature: (JI)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setWriteBufferSize
  (JNIEnv * env, jclass c, jlong h, jlong v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->write_buffer_size = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setMaxOpenFile
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setMaxOpenFile
  (JNIEnv * env, jclass c, jlong h, jint v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->max_open_files = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setLRUBlockCache
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setLRUBlockCache
  (JNIEnv * env, jclass c, jlong h, jlong v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->block_cache = leveldb::NewLRUCache(v);
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setBlockSize
 * Signature: (JI)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setBlockSize
  (JNIEnv * env, jclass c, jlong h, jint v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->block_size = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setBlockRestartInterval
 * Signature: (JI)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setBlockRestartInterval
  (JNIEnv * env, jclass c, jlong h, jint v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->block_restart_interval = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _setCompressionType
 * Signature: (JI)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1setCompressionType
  (JNIEnv * env, jclass c, jlong h, jint v)
{
  leveldb::Options* o = (leveldb::Options*) h; 
  o->compression = (leveldb::CompressionType) v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbOptions
 * Method:    _new
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_voldemort_store_leveldb_leveldbjni_DbOptions__1new
  (JNIEnv * env, jclass c)
{
  leveldb::Options* o;
  o = new leveldb::Options();
  return (jlong) o;
}