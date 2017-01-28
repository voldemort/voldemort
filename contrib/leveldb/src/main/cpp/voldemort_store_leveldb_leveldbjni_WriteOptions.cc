#include <jni.h> 
#include <iostream>
#include "voldemort_store_leveldb_leveldbjni_WriteOptions.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include <string>

/*
 * Class:     voldemort_store_leveldb_leveldbjni_WriteOptions
 * Method:    _new
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_voldemort_store_leveldb_leveldbjni_WriteOptions__1new
  (JNIEnv *env, jclass c)
{
  leveldb::WriteOptions* o;
  o = new leveldb::WriteOptions();
  return (jlong) o;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_WriteOptions
 * Method:    _setSync
 * Signature: (JZ)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_WriteOptions__1setSync
  (JNIEnv *env, jclass c, jlong h, jboolean v)
{
  leveldb::WriteOptions* o = (leveldb::WriteOptions*) h; 
  o->sync = v;
  return true;
}