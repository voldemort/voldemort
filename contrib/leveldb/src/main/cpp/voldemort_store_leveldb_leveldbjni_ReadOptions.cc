#include <jni.h> 
#include <iostream>
#include "voldemort_store_leveldb_leveldbjni_ReadOptions.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include <string>

/*
 * Class:     voldemort_store_leveldb_leveldbjni_ReadOptions
 * Method:    _new
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_voldemort_store_leveldb_leveldbjni_ReadOptions__1new
  (JNIEnv * env, jclass c)
{
  leveldb::ReadOptions* o;
  o = new leveldb::ReadOptions();
  return (jlong) o;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_ReadOptions
 * Method:    _setFillCache
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_ReadOptions__1setFillCache
  (JNIEnv * env, jclass c, jlong h, jboolean v)
{
  leveldb::ReadOptions* o = (leveldb::ReadOptions*) h; 
  o->fill_cache = v;
  return true;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_ReadOptions
 * Method:    _setVerifyChecksums
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_ReadOptions__1setVerifyChecksums
  (JNIEnv * env, jclass c, jlong h, jboolean v)
{
  leveldb::ReadOptions* o = (leveldb::ReadOptions*) h; 
  o->verify_checksums = v;
  return true;    
}