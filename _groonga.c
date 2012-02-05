/*
 * Copyright (c) 2012 Naoya INADA <naoina@kuune.org>
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * 
 */

#include <Python.h>
#include <groonga/groonga.h>

#define MODULE_NAME     "_groonga"
#define TRUE    (1)
#define FALSE   (0)

#if (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 1) || PY_MAJOR_VERSION > 3
# define PY3
#endif

#ifdef PY3
# define INIT_RETURN(m) return(m)
#else
# define INIT_RETURN(m) return
#endif

typedef struct {
    const char *name;
    int value;
} pair;

static pair _groonga_consts[] = {
    /* grn_rc */
    {"SUCCESS", GRN_SUCCESS},
    {"END_OF_DATA", GRN_END_OF_DATA},
    {"UNKNOWN_ERROR", GRN_UNKNOWN_ERROR},
    {"OPERATION_NOT_PERMITTED", GRN_OPERATION_NOT_PERMITTED},
    {"NO_SUCH_FILE_OR_DIRECTORY", GRN_NO_SUCH_FILE_OR_DIRECTORY},
    {"NO_SUCH_PROCESS", GRN_NO_SUCH_PROCESS},
    {"INTERRUPTED_FUNCTION_CALL", GRN_INTERRUPTED_FUNCTION_CALL},
    {"INPUT_OUTPUT_ERROR", GRN_INPUT_OUTPUT_ERROR},
    {"NO_SUCH_DEVICE_OR_ADDRESS", GRN_NO_SUCH_DEVICE_OR_ADDRESS},
    {"ARG_LIST_TOO_LONG", GRN_ARG_LIST_TOO_LONG},
    {"EXEC_FORMAT_ERROR", GRN_EXEC_FORMAT_ERROR},
    {"BAD_FILE_DESCRIPTOR", GRN_BAD_FILE_DESCRIPTOR},
    {"NO_CHILD_PROCESSES", GRN_NO_CHILD_PROCESSES},
    {"RESOURCE_TEMPORARILY_UNAVAILABLE", GRN_RESOURCE_TEMPORARILY_UNAVAILABLE},
    {"NOT_ENOUGH_SPACE", GRN_NOT_ENOUGH_SPACE},
    {"PERMISSION_DENIED", GRN_PERMISSION_DENIED},
    {"BAD_ADDRESS", GRN_BAD_ADDRESS},
    {"RESOURCE_BUSY", GRN_RESOURCE_BUSY},
    {"FILE_EXISTS", GRN_FILE_EXISTS},
    {"IMPROPER_LINK", GRN_IMPROPER_LINK},
    {"NO_SUCH_DEVICE", GRN_NO_SUCH_DEVICE},
    {"NOT_A_DIRECTORY", GRN_NOT_A_DIRECTORY},
    {"IS_A_DIRECTORY", GRN_IS_A_DIRECTORY},
    {"INVALID_ARGUMENT", GRN_INVALID_ARGUMENT},
    {"TOO_MANY_OPEN_FILES_IN_SYSTEM", GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM},
    {"TOO_MANY_OPEN_FILES", GRN_TOO_MANY_OPEN_FILES},
    {"INAPPROPRIATE_I_O_CONTROL_OPERATION", GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION},
    {"FILE_TOO_LARGE", GRN_FILE_TOO_LARGE},
    {"NO_SPACE_LEFT_ON_DEVICE", GRN_NO_SPACE_LEFT_ON_DEVICE},
    {"INVALID_SEEK", GRN_INVALID_SEEK},
    {"READ_ONLY_FILE_SYSTEM", GRN_READ_ONLY_FILE_SYSTEM},
    {"TOO_MANY_LINKS", GRN_TOO_MANY_LINKS},
    {"BROKEN_PIPE", GRN_BROKEN_PIPE},
    {"DOMAIN_ERROR", GRN_DOMAIN_ERROR},
    {"RESULT_TOO_LARGE", GRN_RESULT_TOO_LARGE},
    {"RESOURCE_DEADLOCK_AVOIDED", GRN_RESOURCE_DEADLOCK_AVOIDED},
    {"NO_MEMORY_AVAILABLE", GRN_NO_MEMORY_AVAILABLE},
    {"FILENAME_TOO_LONG", GRN_FILENAME_TOO_LONG},
    {"NO_LOCKS_AVAILABLE", GRN_NO_LOCKS_AVAILABLE},
    {"FUNCTION_NOT_IMPLEMENTED", GRN_FUNCTION_NOT_IMPLEMENTED},
    {"DIRECTORY_NOT_EMPTY", GRN_DIRECTORY_NOT_EMPTY},
    {"ILLEGAL_BYTE_SEQUENCE", GRN_ILLEGAL_BYTE_SEQUENCE},
    {"SOCKET_NOT_INITIALIZED", GRN_SOCKET_NOT_INITIALIZED},
    {"OPERATION_WOULD_BLOCK", GRN_OPERATION_WOULD_BLOCK},
    {"ADDRESS_IS_NOT_AVAILABLE", GRN_ADDRESS_IS_NOT_AVAILABLE},
    {"NETWORK_IS_DOWN", GRN_NETWORK_IS_DOWN},
    {"NO_BUFFER", GRN_NO_BUFFER},
    {"SOCKET_IS_ALREADY_CONNECTED", GRN_SOCKET_IS_ALREADY_CONNECTED},
    {"SOCKET_IS_NOT_CONNECTED", GRN_SOCKET_IS_NOT_CONNECTED},
    {"SOCKET_IS_ALREADY_SHUTDOWNED", GRN_SOCKET_IS_ALREADY_SHUTDOWNED},
    {"OPERATION_TIMEOUT", GRN_OPERATION_TIMEOUT},
    {"CONNECTION_REFUSED", GRN_CONNECTION_REFUSED},
    {"RANGE_ERROR", GRN_RANGE_ERROR},
    {"TOKENIZER_ERROR", GRN_TOKENIZER_ERROR},
    {"FILE_CORRUPT", GRN_FILE_CORRUPT},
    {"INVALID_FORMAT", GRN_INVALID_FORMAT},
    {"OBJECT_CORRUPT", GRN_OBJECT_CORRUPT},
    {"TOO_MANY_SYMBOLIC_LINKS", GRN_TOO_MANY_SYMBOLIC_LINKS},
    {"NOT_SOCKET", GRN_NOT_SOCKET},
    {"OPERATION_NOT_SUPPORTED", GRN_OPERATION_NOT_SUPPORTED},
    {"ADDRESS_IS_IN_USE", GRN_ADDRESS_IS_IN_USE},
    {"ZLIB_ERROR", GRN_ZLIB_ERROR},
    {"LZO_ERROR", GRN_LZO_ERROR},
    {"STACK_OVER_FLOW", GRN_STACK_OVER_FLOW},
    {"SYNTAX_ERROR", GRN_SYNTAX_ERROR},
    {"RETRY_MAX", GRN_RETRY_MAX},
    {"INCOMPATIBLE_FILE_FORMAT", GRN_INCOMPATIBLE_FILE_FORMAT},
    {"UPDATE_NOT_ALLOWED", GRN_UPDATE_NOT_ALLOWED},
    {"TOO_SMALL_OFFSET", GRN_TOO_SMALL_OFFSET},
    {"TOO_LARGE_OFFSET", GRN_TOO_LARGE_OFFSET},
    {"TOO_SMALL_LIMIT", GRN_TOO_SMALL_LIMIT},
    {"CAS_ERROR", GRN_CAS_ERROR},
    {"UNSUPPORTED_COMMAND_VERSION", GRN_UNSUPPORTED_COMMAND_VERSION},

    /* ctx option flag */
    {"CTX_USE_QL", GRN_CTX_USE_QL},
    {"CTX_BATCH_MODE", GRN_CTX_BATCH_MODE},
    {"CTX_PER_DB", GRN_CTX_PER_DB},

    /* grn_encoding */
    {"ENC_DEFAULT", GRN_ENC_DEFAULT},
    {"ENC_NONE", GRN_ENC_NONE},
    {"ENC_EUC_JP", GRN_ENC_EUC_JP},
    {"ENC_UTF8", GRN_ENC_UTF8},
    {"ENC_SJIS", GRN_ENC_SJIS},
    {"ENC_LATIN1", GRN_ENC_LATIN1},
    {"ENC_KOI8R", GRN_ENC_KOI8R},
};

static int
_groonga_init_grn_rc(PyObject *module)
{
    int size = sizeof(_groonga_consts) / sizeof(_groonga_consts[0]);
    int i;

    for (i = 0; i < size; i++) {
        pair p = _groonga_consts[i];

        if (PyModule_AddIntConstant(module, p.name, p.value) == -1) {
            PyErr_SetNone(PyExc_SystemError);
            return -1;
        }
    }

    return 0;
}

typedef struct {
    PyObject_HEAD
    grn_ctx ctx;
    int opened;
} GroongaContext;

static void
GroongaContext_dealloc(GroongaContext *self)
{
    if (self->opened) {
        Py_BEGIN_ALLOW_THREADS
        grn_ctx_fin(&self->ctx);
        Py_END_ALLOW_THREADS
    }

    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
GroongaContext_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    GroongaContext *self;

    self = (GroongaContext *)type->tp_alloc(type, 0);
    if (self != NULL) {
        /* initialize */
    }

    return (PyObject *)self;
}

static int
GroongaContext_init(GroongaContext *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"flags", NULL};
    int flags;
    int rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i", kwlist, &flags)) {
        return -1;
    }

    Py_BEGIN_ALLOW_THREADS
    rc = grn_ctx_init(&self->ctx, flags);
    Py_END_ALLOW_THREADS

    if (rc != GRN_SUCCESS) {
        Py_TYPE(self)->tp_free((PyObject *)self);
        return -1;
    }

    self->opened = TRUE;

    return 0;
}

static PyObject *
GroongaContext_get_encoding(GroongaContext *self)
{
    int encoding;

    encoding = GRN_CTX_GET_ENCODING(&self->ctx);

    return Py_BuildValue("i", encoding);
}

static PyObject *
GroongaContext_set_encoding(GroongaContext *self, PyObject *args, PyObject *kwargs)
{
    int encoding;
    static char *kwlist[] = {"encoding", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i", kwlist, &encoding)) {
        return NULL;
    }

    GRN_CTX_SET_ENCODING(&self->ctx, encoding);

    Py_RETURN_NONE;
}

static PyObject *
GroongaContext_connect(GroongaContext *self, PyObject *args, PyObject *kwargs)
{
    int rc;
    const char *host;
    int port;
    int flags;
    static char *kwlist[] = {"host", "port", "flags", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sii", kwlist, &host, &port, &flags)) {
        return NULL;
    }

    rc = grn_ctx_connect(&self->ctx, host, port, flags);

    return Py_BuildValue("i", rc);
}

static PyObject *
GroongaContext_send(GroongaContext *self, PyObject *args, PyObject *kwargs)
{
    const char *str;
    unsigned int str_len;
    int flags;
    static char *kwlist[] = {"str", "flags", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#i", kwlist, &str, &str_len, &flags)) {
        return NULL;
    }

    grn_ctx_send(&self->ctx, str, str_len, flags);

    Py_RETURN_NONE;
}

static PyObject *
GroongaContext_recv(GroongaContext *self)
{
    char *str;
    unsigned int str_len;
    int flags;

    grn_ctx_recv(&self->ctx, &str, &str_len, &flags);

    return Py_BuildValue("(s#i)", str, str_len, flags);
}

static PyMethodDef GroongaContext_methods[] = {
    {"get_encoding", (PyCFunction)GroongaContext_get_encoding, METH_NOARGS,
     ""},
    {"set_encoding", (PyCFunction)GroongaContext_set_encoding, METH_VARARGS | METH_KEYWORDS,
     ""},
    {"connect", (PyCFunction)GroongaContext_connect, METH_VARARGS | METH_KEYWORDS,
     ""},
    {"send", (PyCFunction)GroongaContext_send, METH_VARARGS | METH_KEYWORDS,
     ""},
    {"recv", (PyCFunction)GroongaContext_recv, METH_NOARGS,
     ""},
    {NULL}, /* Sentinel */
};

static PyTypeObject GroongaContextType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_groonga.Context",        /* tp_name */
    sizeof(GroongaContext),    /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)GroongaContext_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "groonga context objects", /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    GroongaContext_methods,    /* tp_methods */
    0,    /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)GroongaContext_init, /* tp_init */
    0,                         /* tp_alloc */
    GroongaContext_new,        /* tp_new */
};

static PyObject *
_groonga_get_version(PyObject *self)
{
    const char *version;

    version = grn_get_version();

    return PyUnicode_FromString(version);
}

static PyObject *
_groonga_get_package(PyObject *self)
{
    const char *package;

    package = grn_get_package();

    return PyUnicode_FromString(package);
}

static PyMethodDef _groonga_methods[] = {
    {"get_version", (PyCFunction)_groonga_get_version, METH_NOARGS,
     ""},
    {"get_package", (PyCFunction)_groonga_get_package, METH_NOARGS,
     ""},
    {NULL},  /* Sentinel */
};

static void
_groonga_initialize(PyObject *module)
{
    grn_init();

    if (Py_AtExit((void (*)(void))grn_fin) == -1) {
        goto error;
    }

    if (_groonga_init_grn_rc(module) == -1) {
        goto error;
    }

error:
    if (PyErr_Occurred()) {
        PyErr_SetString(PyExc_ImportError, "_groonga: init failed");
    }
}

#ifdef PY3
static struct PyModuleDef _groonga_module = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME, /* name of module */
    NULL,        /* module documentation, may be NULL */
    -1,          /* size of per-interpreter state of the module,
                    or -1 if the module keeps state in global variables. */
    _groonga_methods,
};

PyMODINIT_FUNC
PyInit__groonga(void)
#else
PyMODINIT_FUNC
init_groonga(void)
#endif
{
    PyObject *module = NULL;

    GroongaContextType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&GroongaContextType) < 0) {
        INIT_RETURN(module);
    }

#ifdef PY3
    module = PyModule_Create(&_groonga_module);
#else
    module = Py_InitModule(MODULE_NAME, _groonga_methods);
#endif

    if (module == NULL) {
        INIT_RETURN(module);
    }

    Py_INCREF(&GroongaContextType);
    PyModule_AddObject(module, "Context", (PyObject *)&GroongaContextType);

    _groonga_initialize(module);

    INIT_RETURN(module);
}
