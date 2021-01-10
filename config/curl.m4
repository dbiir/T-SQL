dnl @synopsis AC_CHECK_CURL(version, action-if, action-not)
dnl
dnl @summary check for curl of sufficient version by looking at curl-config
dnl
dnl Defines CURL_LIBS, CURL_CFLAGS. See curl-config(1) man page.
dnl
dnl @category InstalledPackages
dnl @author Akos Maroy <darkeye@tyrell.hu>
dnl @version 2005-09-20
dnl @license AllPermissive

AC_DEFUN([AC_CHECK_CURL], [
  succeeded=no

  if test -z "$CURL_CONFIG"; then
    AC_PATH_PROG(CURL_CONFIG, curl-config, no)
  fi

  if test "$CURL_CONFIG" = "no" ; then
    echo "*** The curl-config script could not be found. Make sure it is"
    echo "*** in your path, and that curl is properly installed."
    echo "*** Or see http://curl.haxx.se/"
  else
    dnl curl-config --version returns "libcurl <version>", thus cut the number
    CURL_VERSION=`$CURL_CONFIG --version | cut -d" " -f2`
    AC_MSG_CHECKING(for curl >= $1)
        VERSION_CHECK=`expr $CURL_VERSION \>\= $1`
        if test "$VERSION_CHECK" = "1" ; then
            AC_MSG_RESULT(yes)
            succeeded=yes

            AC_MSG_CHECKING(CURL_CFLAGS)
            CURL_CFLAGS=`$CURL_CONFIG --cflags`
            AC_MSG_RESULT($CURL_CFLAGS)

            AC_MSG_CHECKING(CURL_LIBS)
            CURL_LIBS=`$CURL_CONFIG --libs`
            AC_MSG_RESULT($CURL_LIBS)
        else
            CURL_CFLAGS=""
            CURL_LIBS=""
            ## If we have a custom action on failure, don't print errors, but
            ## do set a variable so people can do so.
            ifelse([$3], ,echo "can't find curl >= $1",)
        fi

        AC_SUBST(CURL_CFLAGS)
        AC_SUBST(CURL_LIBS)
  fi

  if test $succeeded = yes; then
     ifelse([$2], , :, [$2])
  else
     ifelse([$3], , AC_MSG_ERROR([Library requirements (curl) not met.]), [$3])
  fi
])

