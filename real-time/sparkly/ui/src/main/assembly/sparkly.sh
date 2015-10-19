#!/bin/bash
# VERSION ${project.version}

SCRIPT_FILE=`readlink -f "$0" 2>/dev/null`
if test "$SCRIPT_FILE" = ''
then
  SCRIPT_FILE=`readlink "$0" 2>/dev/null`
  if test "$SCRIPT_FILE" = ''
  then
    SCRIPT_FILE="$0"
  fi
fi

BIN=$(dirname $SCRIPT_FILE)
SPARKLY_HOME=$(dirname $BIN)
CONF=$SPARKLY_HOME/conf


echo "Launching Sparkly ..."
CLASSPATH="$SPARKLY_HOME/bin/*:$SPARKLY_HOME/conf:$SPARKLY_HOME/lib/*"
exec java "-Dsparkly.home=${SPARKLY_HOME}" -cp "$CLASSPATH" Boot "$@"

exit 0