#!/usr/bin/env bash
. /etc/profile
APPNAME=expression-tpm-pipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

EMAILLIST="llamers@mcw.edu"
if [ "$SERVER" == "REED" ]; then
  EMAILLIST="llamers@mcw.edu mtutaj@mcw.edu jrsmith@mcw.edu akwitek@mcw.edu wdemos@mcw.edu"
fi

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar --downloadMetaData "$@" > runDownload.log 2>&1

mailx -s "[$SERVER] expression-tpm-pipeline Run for Meta Data" $EMAILLIST < $APPDIR/logs/downloadSummary.log
