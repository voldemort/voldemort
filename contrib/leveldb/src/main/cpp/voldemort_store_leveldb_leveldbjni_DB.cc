#include <jni.h> 
#include <iostream>
#include "voldemort_store_leveldb_leveldbjni_DB.h"
#include "leveldb/db.h"
// #include "port/port.h"
#include "leveldb/slice.h"
#include <string>

JNIEXPORT jlong JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1open
  (JNIEnv *env, jclass clazz, jobject dbo, jstring js)
{
  if(dbo == NULL) return NULL;
  static jfieldID selfid = NULL;
  if(selfid == NULL){
    jclass cls = (env)->GetObjectClass(dbo);
    selfid = (env)->GetFieldID(cls, "self","J");
  }

  const char *path = env->GetStringUTFChars(js, 0);
  leveldb::DB* db;
  leveldb::Options* options = (leveldb::Options*) env->GetLongField(dbo,selfid);

  leveldb::Status status = leveldb::DB::Open(*options, path, &db);
  env->ReleaseStringUTFChars(js, path);

  return (jlong) db;
}


JNIEXPORT jint JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1put
  (JNIEnv *env, jclass clazz, jlong h, jobject wo, jbyteArray key, jbyteArray value)
{
  static jfieldID selfid = NULL;
  leveldb::WriteOptions* lwo;
  if(wo !=NULL){
    if(selfid == NULL){
      jclass cls = (env)->GetObjectClass(wo);
      selfid = (env)->GetFieldID(cls, "self","J");
    }
    lwo = (leveldb::WriteOptions*) env->GetLongField(wo,selfid);
  }else
    lwo = new leveldb::WriteOptions();

  leveldb::DB* db = (leveldb::DB*) h;

  jboolean isCopy;
  char* kb = (char*)(env)->GetPrimitiveArrayCritical(key,&isCopy);
  int kl = (env)->GetArrayLength(key);
  leveldb::Slice* k = new leveldb::Slice(kb,kl);

  char *vb = (char*)(env)->GetPrimitiveArrayCritical(value,JNI_FALSE);
  int vl = (env)->GetArrayLength(value);
  leveldb::Slice* v = new leveldb::Slice(vb,vl);

  leveldb::Status s = db->Put(*lwo,*k,*v);
  delete k;
  delete v;
  (env)->ReleasePrimitiveArrayCritical(key,kb,0);
  (env)->ReleasePrimitiveArrayCritical(value,vb,0);
  return s.ok();
}


JNIEXPORT jint JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1delete
  (JNIEnv *env, jclass clazz, jlong h, jobject wo, jbyteArray key)
{
  static jfieldID selfid = NULL;
  if(key == NULL) return NULL;
  leveldb::WriteOptions* lwo;
  if(wo !=NULL){
    if(selfid == NULL){
      jclass cls = (env)->GetObjectClass(wo);
      selfid = (env)->GetFieldID(cls, "self","J");
    }
    lwo = (leveldb::WriteOptions*) env->GetLongField(wo,selfid);
  }else
    lwo = new leveldb::WriteOptions();

  leveldb::DB* db = (leveldb::DB*) h;

  char* kb = (char*)(env)->GetPrimitiveArrayCritical(key,JNI_FALSE);
  int kl = (env)->GetArrayLength(key);
  leveldb::Slice* k = new leveldb::Slice(kb,kl);

  leveldb::Status s = db->Delete(*lwo,*k);
  delete k;
  (env)->ReleasePrimitiveArrayCritical(key,kb,0);
  return s.ok();
}


JNIEXPORT jbyteArray JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1get
  (JNIEnv *env, jclass clazz, jlong h, jobject ro, jbyteArray key)
{
  static jfieldID selfid = NULL;
  leveldb::ReadOptions* lro;
  if(ro != NULL){
    if(selfid == NULL){
      jclass cls = (env)->GetObjectClass(ro);
      selfid = (env)->GetFieldID(cls, "self","J");
    }
    lro = (leveldb::ReadOptions*) env->GetLongField(ro,selfid);
  }else
    lro = new leveldb::ReadOptions();
  
  leveldb::DB* db = (leveldb::DB*) h;

  char* kb = (char*)(env)->GetPrimitiveArrayCritical(key,JNI_FALSE);
  int kl = (env)->GetArrayLength(key);
  leveldb::Slice* k = new leveldb::Slice(kb,kl);

  std::string retval;
  leveldb::Status s = db->Get(*lro, *k, &retval);
  delete k;
  (env)->ReleasePrimitiveArrayCritical(key,kb,0);

  int jbas = retval.length();
  jbyteArray jba = (env)->NewByteArray(jbas);
  (env)->SetByteArrayRegion(jba,0,(jsize)jbas,(jbyte*)retval.data());

  return jba;
}


JNIEXPORT void JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1close
  (JNIEnv *env, jclass c, jlong h)
{
  leveldb::DB* db = (leveldb::DB*) h;
  delete db;
}


/*
 * Class:     voldemort_store_leveldb_leveldbjni_DB
 * Method:    _write
 * Signature: (JLvoldemort/leveldbjni/WriteOptions;Lvoldemort/leveldbjni/WriteBatch;)I
 */
JNIEXPORT jint JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1write
  (JNIEnv *env , jclass c, jlong h, jobject wo, jobject wb)
{
  if(wb == NULL) return -1;

  static jfieldID selfid = NULL;
  leveldb::WriteOptions* lwo;
  if(wo !=NULL){
    if(selfid == NULL){
      jclass cls = (env)->GetObjectClass(wo);
      selfid = (env)->GetFieldID(cls, "self","J");
    }
    lwo = (leveldb::WriteOptions*) env->GetLongField(wo,selfid);
  }else
    lwo = new leveldb::WriteOptions();

  static jfieldID selfwbid = NULL;
  if(selfid == NULL){
    jclass cls = (env)->GetObjectClass(wb);
    selfwbid = (env)->GetFieldID(cls, "self","J");
  }
  leveldb::WriteBatch* lwb = 
    (leveldb::WriteBatch*) env->GetLongField(wb,selfwbid);
    
  leveldb::DB* db = (leveldb::DB*) h;

  leveldb::Status s = db->Write(*lwo, lwb);

  return s.ok();
}


/*
 * Class:     voldemort_store_leveldb_leveldbjni_DB
 * Method:    _newIterator
 * Signature: (Lvoldemort/leveldbjni/ReadOptions;)J
 */
JNIEXPORT jlong JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1newIterator
  (JNIEnv *env , jclass c , jlong h, jobject ro)
{
  static jfieldID selfid = NULL;
  leveldb::ReadOptions* lro;
  if(ro != NULL){
    if(selfid == NULL){
      jclass cls = (env)->GetObjectClass(ro);
      selfid = (env)->GetFieldID(cls, "self","J");
    }
    lro = (leveldb::ReadOptions*) env->GetLongField(ro,selfid);
  }else
    lro = new leveldb::ReadOptions();
  
  leveldb::DB* db = (leveldb::DB*) h;
  return (jlong) db->NewIterator(leveldb::ReadOptions());
}


/*
 * Class:     voldemort_store_leveldb_leveldbjni_DB
 * Method:    _getProperty
 * Signature: (JLjava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1getProperty
  (JNIEnv *env , jclass c, jlong h, jstring pn)
{
  leveldb::DB* db = (leveldb::DB*) h;
  const char *prop = env->GetStringUTFChars(pn, 0);
  leveldb::Slice ps = prop;
  std::string s;
  
  db->GetProperty(ps,&s);
  // if(s == NULL)return NULL;
  
  env->ReleaseStringUTFChars(pn, prop);
  jstring js = env->NewStringUTF(s.c_str());
  return js;
}


/*
 * Class:     voldemort_store_leveldb_leveldbjni_DB
 * Method:    _destroyDB
 * Signature: (Ljava/lang/String;Lvoldemort/leveldbjni/DbOptions;)I
 */
JNIEXPORT jint JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1destroyDB
  (JNIEnv *env , jclass c, jstring js, jobject o)
{
  static jfieldID selfid = NULL;
  leveldb::Options* lo;
  if(o !=NULL){
    if(selfid == NULL){
      jclass cls = (env)->GetObjectClass(o);
      selfid = (env)->GetFieldID(cls, "self","J");
    }
    lo = (leveldb::Options*) env->GetLongField(o,selfid);
  }else
    lo = new leveldb::Options();

  const char *path = env->GetStringUTFChars(js, 0);
  leveldb::Status s = leveldb::DestroyDB(path,*lo);
  env->ReleaseStringUTFChars(js, path);

  return s.ok();
}


/*
 * Class:     voldemort_store_leveldb_leveldbjni_DB
 * Method:    _compactRange
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_voldemort_store_leveldb_leveldbjni_DB__1compactRange
  (JNIEnv * env, jclass c, jlong h, jbyteArray start, jbyteArray end)
{
  leveldb::Slice* s = NULL;
  leveldb::Slice* e = NULL;
  if(start !=NULL){
    char* sb = (char*)(env)->GetPrimitiveArrayCritical(start,JNI_FALSE);
    int sl = (env)->GetArrayLength(start);
    s = new leveldb::Slice(sb,sl);
  }
  if(start !=NULL){
    char* eb = (char*)(env)->GetPrimitiveArrayCritical(end,JNI_FALSE);
    int el = (env)->GetArrayLength(end);
    e = new leveldb::Slice(eb,el);
  }

  leveldb::DB* db = (leveldb::DB*) h;
  db->CompactRange(s,e);
  delete s;
  delete e;
}

