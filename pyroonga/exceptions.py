# -*- coding: utf-8 -*-

# Copyright (c) 2012 Naoya INADA <naoina@kuune.org>
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.


__author__ = "Naoya INADA <naoina@kuune.org>"

import _groonga

error_messages = {
    _groonga.END_OF_DATA: "end of data",
    _groonga.UNKNOWN_ERROR: "unknown error",
    _groonga.OPERATION_NOT_PERMITTED: "operation not permitted",
    _groonga.NO_SUCH_FILE_OR_DIRECTORY: "no such file or directory",
    _groonga.NO_SUCH_PROCESS: "no such process",
    _groonga.INTERRUPTED_FUNCTION_CALL: "interrupted function call",
    _groonga.INPUT_OUTPUT_ERROR: "input output error",
    _groonga.NO_SUCH_DEVICE_OR_ADDRESS: "no such device or address",
    _groonga.ARG_LIST_TOO_LONG: "arg list too long",
    _groonga.EXEC_FORMAT_ERROR: "exec format error",
    _groonga.BAD_FILE_DESCRIPTOR: "bad file descriptor",
    _groonga.NO_CHILD_PROCESSES: "no child processes",
    _groonga.RESOURCE_TEMPORARILY_UNAVAILABLE: "resource temporarily unavailable",
    _groonga.NOT_ENOUGH_SPACE: "not enough space",
    _groonga.PERMISSION_DENIED: "permission denied",
    _groonga.BAD_ADDRESS: "bad address",
    _groonga.RESOURCE_BUSY: "resource busy",
    _groonga.FILE_EXISTS: "file exists",
    _groonga.IMPROPER_LINK: "improper link",
    _groonga.NO_SUCH_DEVICE: "no such device",
    _groonga.NOT_A_DIRECTORY: "not a directory",
    _groonga.IS_A_DIRECTORY: "is a directory",
    _groonga.INVALID_ARGUMENT: "invalid argument",
    _groonga.TOO_MANY_OPEN_FILES_IN_SYSTEM: "too many open files in system",
    _groonga.TOO_MANY_OPEN_FILES: "too many open files",
    _groonga.INAPPROPRIATE_I_O_CONTROL_OPERATION: "inappropriate i o control operation",
    _groonga.FILE_TOO_LARGE: "file too large",
    _groonga.NO_SPACE_LEFT_ON_DEVICE: "no space left on device",
    _groonga.INVALID_SEEK: "invalid seek",
    _groonga.READ_ONLY_FILE_SYSTEM: "read only file system",
    _groonga.TOO_MANY_LINKS: "too many links",
    _groonga.BROKEN_PIPE: "broken pipe",
    _groonga.DOMAIN_ERROR: "domain error",
    _groonga.RESULT_TOO_LARGE: "result too large",
    _groonga.RESOURCE_DEADLOCK_AVOIDED: "resource deadlock avoided",
    _groonga.NO_MEMORY_AVAILABLE: "no memory available",
    _groonga.FILENAME_TOO_LONG: "filename too long",
    _groonga.NO_LOCKS_AVAILABLE: "no locks available",
    _groonga.FUNCTION_NOT_IMPLEMENTED: "function not implemented",
    _groonga.DIRECTORY_NOT_EMPTY: "directory not empty",
    _groonga.ILLEGAL_BYTE_SEQUENCE: "illegal byte sequence",
    _groonga.SOCKET_NOT_INITIALIZED: "socket not initialized",
    _groonga.OPERATION_WOULD_BLOCK: "operation would block",
    _groonga.ADDRESS_IS_NOT_AVAILABLE: "address is not available",
    _groonga.NETWORK_IS_DOWN: "network is down",
    _groonga.NO_BUFFER: "no buffer",
    _groonga.SOCKET_IS_ALREADY_CONNECTED: "socket is already connected",
    _groonga.SOCKET_IS_NOT_CONNECTED: "socket is not connected",
    _groonga.SOCKET_IS_ALREADY_SHUTDOWNED: "socket is already shutdowned",
    _groonga.OPERATION_TIMEOUT: "operation timeout",
    _groonga.CONNECTION_REFUSED: "connection refused",
    _groonga.RANGE_ERROR: "range error",
    _groonga.TOKENIZER_ERROR: "tokenizer error",
    _groonga.FILE_CORRUPT: "file corrupt",
    _groonga.INVALID_FORMAT: "invalid format",
    _groonga.OBJECT_CORRUPT: "object corrupt",
    _groonga.TOO_MANY_SYMBOLIC_LINKS: "too many symbolic links",
    _groonga.NOT_SOCKET: "not socket",
    _groonga.OPERATION_NOT_SUPPORTED: "operation not supported",
    _groonga.ADDRESS_IS_IN_USE: "address is in use",
    _groonga.ZLIB_ERROR: "zlib error",
    _groonga.LZO_ERROR: "lzo error",
    _groonga.STACK_OVER_FLOW: "stack over flow",
    _groonga.SYNTAX_ERROR: "syntax error",
    _groonga.RETRY_MAX: "retry max",
    _groonga.INCOMPATIBLE_FILE_FORMAT: "incompatible file format",
    _groonga.UPDATE_NOT_ALLOWED: "update not allowed",
    _groonga.TOO_SMALL_OFFSET: "too small offset",
    _groonga.TOO_LARGE_OFFSET: "too large offset",
    _groonga.TOO_SMALL_LIMIT: "too small limit",
    _groonga.CAS_ERROR: "cas error",
    _groonga.UNSUPPORTED_COMMAND_VERSION: "unsupported command version",
    }


class GroongaError(Exception):
    def __init__(self, err):
        self.msg = error_messages.get(err) or err

    def __str__(self):
        return self.msg
