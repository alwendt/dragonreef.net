#!/bin/sh
# Tomcat's CGI servlet invokes scripts reliably but does not feed request data
# directly to native executables. Relay exactly CONTENT_LENGTH bytes.
if [ -n "$CONTENT_LENGTH" ]; then
  dd bs=1 count="$CONTENT_LENGTH" 2>/dev/null |
    /opt/tomcat/webapps/ROOT/WEB-INF/cgi-bin/edges.bin
else
  /opt/tomcat/webapps/ROOT/WEB-INF/cgi-bin/edges.bin
fi
