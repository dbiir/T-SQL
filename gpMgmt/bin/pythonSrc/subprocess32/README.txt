This is a backport of the Python 3.2 subprocess module for use on
Python versions 2.4 through 2.7.

It includes many important bug fixes including a C extension module used
internally to handle the code path between fork() and exec().  This version
is reliable when an application is using threads.

Refer to the Python 3.2 documentation for usage information:
 http://docs.python.org/3.2/library/subprocess.html

Timeout support backported from Python 3.3 is also included.

Bugs?  Try to reproduce them on the latest Python 3.x itself and file bug
reports on http://bugs.python.org/.  Add gregory.p.smith to the Nosy list.

-- Gregory P. Smith  greg@krypto.org
