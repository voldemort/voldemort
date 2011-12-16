#include <jni.h> 
#include <iostream>
#include "voldemort_store_leveldb_leveldbjni_DbIterator.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include <string>

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _seekToFirst
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1seekToFirst
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  it->SeekToFirst();
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _seek
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1seek
  (JNIEnv * env, jclass c, jlong h, jbyteArray key)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  char* kb = (char*)(env)->GetPrimitiveArrayCritical(key,JNI_FALSE);
  int kl = (env)->GetArrayLength(key);
  leveldb::Slice* k = new leveldb::Slice(kb,kl);
  it->Seek(*k);
  delete k;
  (env)->ReleasePrimitiveArrayCritical(key,kb,0);
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _next
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1next
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  it->Next();
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _valid
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1valid
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  it->Valid();
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _key
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1key
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  leveldb::Slice s = it->key();
  int jbas = s.size();
  jbyteArray jba = (env)->NewByteArray(jbas);
  (env)->SetByteArrayRegion(jba,0,(jsize)jbas,(jbyte*)s.data());
  return jba;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _value
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1value
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  leveldb::Slice s = it->value();
  int jbas = s.size();
  jbyteArray jba = (env)->NewByteArray(jbas);
  (env)->SetByteArrayRegion(jba,0,(jsize)jbas,(jbyte*)s.data());
  return jba;
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _status
 * Signature: (J)Ljava/lang/String
{
  
}
 */
JNIEXPORT jstring JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1status
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  leveldb::Status s = it->status();
  if(s.ok()) return NULL;
  else return env->NewStringUTF(s.ToString().data());
}

/*
 * Class:     voldemort_store_leveldb_leveldbjni_DbIterator
 * Method:    _close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_voldemort_store_leveldb_leveldbjni_DbIterator__1close
  (JNIEnv * env, jclass c, jlong h)
{
  leveldb::Iterator* it = (leveldb::Iterator*)h;
  delete it;
}