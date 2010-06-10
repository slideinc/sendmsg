/*
 * Copyright (c) 2005-2010 Slide, Inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *     * Neither the name of the author nor the names of other
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <Python.h>
#include <sys/types.h>
#include <sys/socket.h>

static PyObject *sendmsg_sendmsg(PyObject *, PyObject *, PyObject *);
static PyObject *sendmsg_recvmsg(PyObject *, PyObject *, PyObject *);

static PyMethodDef sendmsg_methods[] = {
    {"sendmsg", (PyCFunction)sendmsg_sendmsg, METH_VARARGS|METH_KEYWORDS, NULL},
    {"recvmsg", (PyCFunction)sendmsg_recvmsg, METH_VARARGS|METH_KEYWORDS, NULL},
    {NULL, NULL, 0, NULL}
};

static PyObject *sendmsg_sendmsg(PyObject *self, PyObject *args, 
				 PyObject *keywds) {
    int       fd = -1;
    int       flags = 0;
    int       ret = -1;
    char     *cmsg_buf = NULL;
    struct    msghdr msg;
    struct    iovec iov[1];
    PyObject *ancillary = NULL;
    static char *kwlist[] = {"fd", "data", "flags","ancillary",NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "it#|iO", kwlist,
				     &fd,
				     &iov[0].iov_base,
				     &iov[0].iov_len,
				     &flags,
				     &ancillary))
        return NULL;
    
    msg.msg_name = NULL;
    msg.msg_namelen = 0;

    msg.msg_iov = iov;
    msg.msg_iovlen = 1;

    msg.msg_control = NULL;
    msg.msg_controllen = 0;

    msg.msg_flags = 0;

    if(ancillary) {
	struct cmsghdr *cur;
        unsigned char *data, *cmsg_data;
        int data_len, type, level;

        if(!PyArg_ParseTuple(ancillary, "iit#",
                &level,
                &type,
                &data,
                &data_len)) {
            return NULL;
        }
        cmsg_buf = malloc(CMSG_SPACE(data_len));
        if (!cmsg_buf) {
            return PyErr_NoMemory();
        }
        msg.msg_control = cmsg_buf;
        msg.msg_controllen = CMSG_SPACE(data_len);

        cur = CMSG_FIRSTHDR(&msg);
        cur->cmsg_level = level;
        cur->cmsg_type = type;
        cur->cmsg_len = CMSG_LEN(data_len);
        cmsg_data = CMSG_DATA(cur);
        memcpy(cmsg_data, data, data_len);
        msg.msg_controllen = cur->cmsg_len; // ugh weird C API. CMSG_SPACE includes alignment, unline CMSG_LEN
    }

    ret = sendmsg(fd, &msg, flags);
    free(cmsg_buf); // this is initialized to NULL and then, optionally, to return value of malloc
    if(ret < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }

    return Py_BuildValue("i", ret);
}

static PyObject *sendmsg_recvmsg(PyObject *self, PyObject *args, PyObject *keywds) {
    int fd;
    int flags=0;
    size_t maxsize=8192;
    size_t cmsg_size=4*1024; // enough to store all file descriptors
    int ret;
    struct msghdr msg;
    struct iovec iov[1];
    unsigned char *cmsgbuf;
    PyObject *ancillary;

    static char *kwlist[] = {"fd", "flags", "maxsize", "cmsg_size", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "i|iii", kwlist,
            &fd, &flags, &maxsize, &cmsg_size)) {
        return NULL;
    }
    cmsg_size = CMSG_SPACE(cmsg_size);

    msg.msg_name = NULL;
    msg.msg_namelen = 0;

    iov[0].iov_len = maxsize;
    iov[0].iov_base = malloc(maxsize);
    if (!iov[0].iov_base) {
        return PyErr_NoMemory();
    }
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;

    cmsgbuf = malloc(cmsg_size);
    if (!cmsgbuf) {
        return PyErr_NoMemory();
    }
    memset(cmsgbuf, 0, cmsg_size);
    msg.msg_control = cmsgbuf;
    msg.msg_controllen = cmsg_size;

    ret = recvmsg(fd, &msg, flags);
    if (ret < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        free(iov[0].iov_base);
        free(cmsgbuf);
        return NULL;
    }

    ancillary = PyList_New(0);
    if (!ancillary) {
        free(iov[0].iov_base);
        free(cmsgbuf);
        return NULL;
    }

    {
        struct cmsghdr *cur;

        for (cur=CMSG_FIRSTHDR(&msg); cur; cur=CMSG_NXTHDR(&msg, cur)) {
            PyObject *entry;

            assert(cur->cmsg_len >= sizeof(struct cmsghdr)); // no null ancillary data messages?

            entry = Py_BuildValue("(iis#)",
                        cur->cmsg_level,
                        cur->cmsg_type,
                        CMSG_DATA(cur),
                        cur->cmsg_len - sizeof(struct cmsghdr));
            if (PyList_Append(ancillary, entry) < 0) {
                Py_DECREF(ancillary);
                Py_DECREF(entry);
                free(iov[0].iov_base);
                free(cmsgbuf);
                return NULL;
            }
        }
    }

    {
        PyObject *r;
        r = Py_BuildValue("s#iO",
                iov[0].iov_base, ret,
                msg.msg_flags,
                ancillary);
        free(iov[0].iov_base);
        free(cmsgbuf);
        return r;
    }
}


DL_EXPORT(void) initsendmsg(void) {
    PyObject *m;
    m = Py_InitModule("sendmsg", sendmsg_methods);
    if(-1 == PyModule_AddIntConstant(m, "SCM_RIGHTS", SCM_RIGHTS)) {
        return;
    }
}

