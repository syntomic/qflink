#include <Python.h>
#include "cn_syntomic_ffi_FFIDemo.h"

static PyObject *day_of_week; // 导入一次、反复使用的 Python 函数

/* System.load 加载这个库时，JVM 会先调用 JNI_OnLoad：解释器在这里启动，只启动一次 */
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    Py_Initialize();
    PyRun_SimpleString("import sys; sys.path.insert(0, 'src/main/python')");

    PyObject *module = PyImport_ImportModule("day_of_week");
    day_of_week = module ? PyObject_GetAttrString(module, "day_of_week") : NULL;
    Py_XDECREF(module);
    if (day_of_week == NULL) {
        PyErr_Print();
        return JNI_ERR;
    }

    // Py_Initialize 之后，当前线程一直拿着 GIL；交出来，其他线程才能调用 Python
    PyEval_SaveThread();
    return JNI_VERSION_1_8;
}

JNIEXPORT jint JNICALL Java_cn_syntomic_ffi_FFIDemo_dayOfWeek(JNIEnv *env, jobject self,
                                                              jstring date) {
    // ① Java 的 String -> C 的字符串
    const char *text = (*env)->GetStringUTFChars(env, date, NULL);

    // ② 拿到 GIL：同一时刻，只有拿着它的线程能执行 Python
    PyGILState_STATE gil = PyGILState_Ensure();

    // ③ C 字符串 -> Python 的 str，调用 day_of_week(text)
    PyObject *result = PyObject_CallFunction(day_of_week, "s", text);

    // ④ Python 的 int -> C 的 long；Python 抛了异常就转成 Java 异常
    long weekday = -1;
    if (result != NULL) {
        weekday = PyLong_AsLong(result);
        Py_DECREF(result);
    } else {
        PyErr_Print();
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/RuntimeException"),
                         "Python 函数执行失败");
    }

    // ⑤ 归还 GIL，释放字符串
    PyGILState_Release(gil);
    (*env)->ReleaseStringUTFChars(env, date, text);
    return (jint)weekday;
}
