#!/usr/bin/env python
#
# Name: backup-ora-coh-nfs.py
#
# Function: This script backup oracle in backup set using nfs mount. 
#           It can do incremental backup and use Oracle recovery catalog
#           It can do archive log backup only. The retention time is in days.
#           If the retention time is unlimit, specify unlimit. It only deletes
#           the backup files. It won't clean RMMAN catalog. Oracle cleans its 
#           RMAN catalog. 
# Coding: utf-8
#
################################################################

# # Usage standard output
from __future__ import absolute_import
from io import open
def show_usage():
    print u"usage: python backup-ora-coh-nfs.py [--help] -r <RMAN login> -h <host> -o <Oracle_sid> -a <archive only> -i <incremental level> -m <mount-prefix> -n <number of mounts> -p <number of channels> -e <retention> -l <archive log keep days>" 
    print u" -r : RMAN login (example: \"rman target /\", optional)"
    print u" -h : host (optional)"
    print u" -o : ORACLE_SID"
    print u" -a : arch (yes means archivelogonly, no means database backup plus archivelog)"
    print u" -i : Incremental level"
    print u" -m : mount-prefix (like /mnt/ora)"
    print u" -n : number of mounts"
    print u" -p : number of channels (Optional, default is 4)"
    print u" -e : Retention time (days to retain the backups)"
    print u" -l : Archive logs retain days (days to retain the local archivelogs before deleting them. default is 1 day)"


# # Imports
import re
import os
import sys
import argparse
import subprocess
from subprocess import call
from datetime import datetime


# # Command Line Argument Parsing
rmanlogin = u''
host = u''
dbname = u''
arch = False
level = -1
mount = u''
num = -1
parallel = 4
retday = -1
archretday = 1

archivelogonly = False

argv = sys.argv[1:]
argv = [i.lower() for i in argv]
if u'--help' in argv or u'help' in argv or u'-help' in argv:
    show_usage()
    print u"exit"
    exit(0)
parser = argparse.ArgumentParser(description = u"Parser for backup-ora-coh-nfs", conflict_handler=u'resolve')

parser.add_argument(u"-r", u"--rmanlogin", help = u"RMAN login (example: \"rman target /\", optional)", 
                    required = False, default = u"")
parser.add_argument(u"-h", u"--host", help = u"host (optional)", 
                    required = False, default = u"")
parser.add_argument(u"-o", u"--dbname", help = u"ORACLE_SID", 
                    required = True, default = u"")
parser.add_argument(u"-a", u"--arch", help = u"arch (yes means archivelogonly, no means database backup plus archivelog)", 
                    required = True, default = u"")
parser.add_argument(u"-i", u"--level", help = u"increment level",
                    required = False, default = u"0")
parser.add_argument(u"-m", u"--mount", help = u"mount-prefix (like /mnt/ora)",
    					required = True, default = u"")
parser.add_argument(u"-n", u"--num", help = u"number of mounts",
    					required = True, default = u"")
parser.add_argument(u"-p", u"--parallel", help = u"number of channels (Optional, default is 4)",
    					required = False, default = u"4")
parser.add_argument(u"-e", u"--retday", help = u"Retention time (days to retain the backups)",
    					required = True, default= u"")
parser.add_argument(u"-l", u"--archretday", help = u"Archive logs retain days (days to retain the local archivelogs before deleting them. default is 1 day)",
    					required = False, default= u"1")

# Check required parameters
args = parser.parse_args(argv)

rmanlogin = args.rmanlogin
host = args.host
if host == u'':
    host = u"hostname -s"

if rmanlogin == u'':
    rmanlogin = u"rman target /"
dbname = args.dbname
mount = args.mount
try:
    num = int(args.num)
except ValueError:
    print u"Invalid input for number of mounts"
    show_usage()
    exit(1)
if (num < 1):
    print u"Invalid input for number of mounts"
    show_usage()
    exit(1)
    
try:
    parallel = int(args.parallel)
except ValueError:
    print u"Invalid input for number of channels"
    show_usage()
    exit(1)
if (parallel < 1):
    print u"Invalid input for number of channels"
    show_usage()
    exit(1)
        
try:
    retday = int(args.retday)
except ValueError:
    print u"Invalid input for retention time"
    show_usage()
    exit(1)
if (retday < 1):
    print u"Invalid input for retention time"
    show_usage()
    exit(1)
    
print args.arch.lower()
if (args.arch.lower() == u"no" or args.arch.lower() == u"arch"):
    print u"only backup archive logs"
    archivelogonly = True
else:
    print u"will backup database backup plus archive logs"
    try:
        level = int(args.level)
    except ValueError:
        print u"incremental level is set to be {0}. Backup won't start" .format(args.level)
        print u"incremental backup level needs to be either 0 or 1\n"
        show_usage()
        exit(1)
    if (level != 0 and level != 1):
        print u"incremental level is set to be {0}. Backup won't start" .format(args.level)
        print u"incremental backup level needs to be either 0 or 1\n"
        show_usage()
        exit(1)
            
    try:
        archretday = int(args.archretday)
    except ValueError:
        print u"archive retention day is set to be {0}. Backup won't start" .format(args.archretday)
        print u"please enter a positive integer for the archive retention day"
        show_usage()
        exit(1)
    if (archretday < 1):
        print u"archive retention day is set to be {0}. Backup won't start" .format(args.archretday)
        print u"please enter a positive integer for the archive retention day"
        show_usage()
        exit(1)


# # Set Up
def setup():
    print u"rmanlogin is \{0}\"".format(rmanlogin)
    print u"rmanlogin syntax can be like \"rman target /\" or "
    print u"\"rman target sys/<password>@<database connect string> catalog <user>/<password>@<catalog>\""
    DATE_SUFFIX = datetime.now().strftime(u"%Y%m%d%H%M%S")
    cwd = os.getcwdu()
    if os.path.isdir(cwd+u"/log/{0}".format(host)) == False:
        print u"{0}/log/{1} does not exist, create it".format(cwd, host)
        try:
            os.system(u"mkdir -p {0}/log/{1}".format(cwd, host))
        except OSError:
            print u"Exception created with create new directory ./log"
            exit(1)
    
    runlog = u'{0}/log/{1}/{2}.{3}.log'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(runlog):
        os.system(u'touch {0}'.format(runlog))
        print runlog
    runlog = open(runlog, u"a")

    rmanlog = u'{0}/log/{1}/{2}.rman.{3}.log'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanlog):
        os.system(u'touch {0}'.format(rmanlog))
        print rmanlog
    rmanlog = open(rmanlog, u"a")

    rmanloga = u'{0}/log/{1}/{2}.archive.{3}.log'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanloga):
        os.system(u'touch {0}'.format(rmanloga))
        print rmanloga
    rmanloga = open(rmanloga, u"a")

    rmanfiled = u'{0}/log/{1}/{2}.rman.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfiled):
        os.system(u'touch {0}'.format(rmanfiled))
        print rmanfiled
    rmanfiled = open(rmanfiled, u"a")

    rmanfiled_b = u'{0}/log/{1}/{2}.rman_b.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfiled_b):
        os.system(u'touch {0}'.format(rmanfiled_b))
        print rmanfiled_b
    rmanfiled_b = open(rmanfiled_b, u"a")

    rmanfiled_e = u'{0}/log/{1}/{2}.rman_e.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfiled_e):
        os.system(u'touch {0}'.format(rmanfiled_e))
        print rmanfiled_e
    rmanfiled_e = open(rmanfiled_e, u"a")

    rmanfilea = u'{0}/log/{1}/{2}.archive.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfilea):
        os.system(u'touch {0}'.format(rmanfilea))
        print rmanfilea
    rmanfilea = open(rmanfilea, u"a")

    rmanfilea_b = u'{0}/log/{1}/{2}.archive_b.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfilea_b):
        os.system(u'touch {0}'.format(rmanfilea_b))
        print rmanfilea_b
    rmanfilea_b = open(rmanfilea_b, u"a")

    rmanfilea_e = u'{0}/log/{1}/{2}.archive_e.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfilea_e):
        os.system(u'touch {0}'.format(rmanfilea_e))
        print rmanfilea_e
    rmanfilea_e = open(rmanfilea_e, u"a")
    
    #trim log directory
    temp = u"{}"
    cmd = u"find {}/log/{} -type f -mtime +7 -exec /bin/rm {} \;".format(cwd, host, temp)
    output = subprocess.check_output(cmd)

    if (len(output) != 0):
        runlog.write(u"del old logs in {}/log/{} failed".format(cwd, host))
        print u"del old logs in {}/log/{} failed".format(cwd, host)
        exit(2)
    
    ##ps -ef
    print u"check whether this database is up running"
    cmd = [u"ps", u"-ef"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    output = o.decode(u'ascii')
    if(len(e.decode(u'ascii'))== 0):
        output = output.splitlines()
    else:
        print e
        print u"Error when running ps -ef"
        exit(2)
    
    result = []
    ##grep pmon | awk '{print $8}'
    for line in output:
        if re.search(u"pmon", line):
            result.append(line.split( )[8])
    arroid = []      
    ##grep -i $dbname | awk -F "pmon" 'print {$2}'
    for line in result:
        if re.search(u"{0}".format(dbname), line):
            arroid.append(line.split(u"pmon")[2])
    up = False
    print dbname
    yes_oracle_sid = u""
    
    for i in arroid:
        oracle_sid = i
        oracle_sid = oracle_sid[1:]
        print oracle_sid
        lastc = oracle_sid[-1]
        print lastc
        if (oracle_sid == dbname):
            print u"Oracle database {0} is up on {1}. Backup can start".format(dbname, host)
            yes_orcale_sid = dbname
            up = True
        elif (re.search(ur"^[0-9]+$", lastc)):
            if(oracle_sid[:-1] == dbname):
                print u"Oracle database {0} is up on {1}. Backup can start".format(dbname, host)
                yes_oracle_sid=oracle_sid
                up = True
    if (up == False):
        print u"Oracle database {0} is not up. Backup cannot start".format(dbname)
        exit(2)
    print u"get ORACLE_HOME"
    cmd = [u"grep", u"-i", u"{0}".format(yes_oracle_sid), u'/etc/oratab']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    oratabinfo = o.decode(u'ascii')
    if(len(e.decode(u'ascii'))== 0):
        oratabinfo = oratabinfo.splitlines()
    else:
        print e
        print u"Error when running ps -ef"
        exit(2)
    if oratabinfo == u"" or len(oratabinfo) == 0:
        print u"No Oracle sid {0} information in /etc/oratab. Cannot determine ORACLE_HOME".format(yes_oracle_sid)
        exit(2)
    k = 0
    arrinfo = oratabinfo
    oracle_home = u""
    for i in xrange(len(oratabinfo)):
        #`echo ${arrinfo[$i]} | awk -F ":" '{print $1}'`
        orasidintab = append(arrinfo[i].split(u':')[1])
        #`echo ${arrinfo[$i]} | awk -F ":" '{print $2}'`
        orahomeintab=append(arrinfo[i].split(u':')[2])
        if (orasidintab[0] == yes_oracle_sid):    
            oracle_home=orahomeintab
            #export ORACLE_HOME=$oracle_home
            os.environ[u"ORACLE_HOME"] = oracle_home
            #export PATH=$PATH:$ORACLE_HOME/bin
            os.environ[u"PATH"] = u"PATH:{0}/bin".format(oracle_home)
            k=1
        print u"orasidintab is {0}". format(orasidintab)
    if (k ==0):
        print u"No Oracle sid {0} information in /etc/oratab. Cannot determine ORACLE_HOME".format(dbname)
        exit(2)
    else:
        print u"ORACLE_HOME is {0}".format(ORACLE_HOME)
    os.environ[u"ORACLE_SID"] = yes_oracle_sid
    return runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e


# # Create_rmanfile_all
def create_rmanfile_all(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e):
    runlog.write(u'Create rman file')
    rmanfiled_b.write(u'CONFIGURE DEFAULT DEVICE TYPE TO disk;')
    rmanfilea_b.write(u'CONFIGURE DEFAULT DEVICE TYPE TO disk;')
    rmanfiled_b.write(u'CONFIGURE CONTROLFILE AUTOBACKUP ON;')
    rmanfilea_b.write(u'CONFIGURE CONTROLFILE AUTOBACKUP OFF;')
    rmanfiled_b.write(u'CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO \'{{0}}1/{1}/{2}/%d_%F.ctl\';'.format(mount, host, dbname))
    rmanfiled_b.write(u'   ')
    rmanfilea_b.write(u'   ')
    rmanfiled_b.write(u'RUN {')
    rmanfilea_b.write(u'RUN {')

    i=1
    j=1
    while i <= num:
        if os.path.ismount(u"{0}{1}".format(mount, i)):
            print u"{0}{1} is mount point".format(mount, i)
            print u" "
            if os.path.isdir(u"{0}{1}/{2}/{3}".format(mount, i, host, dbname)) == False:
                print u"Directory {0}{1}/{2}/{3} does not exist, create it".format(mount, i, host, dbname)
                try:
                    os.system(u"mkdir -p {0}{1}/{2}/{3}".format(mount, i, host, dbname))
                except OSError:
                    print u"Exception created with create new directory {0}{1}/{2}/{3}".format(mount, i, host, dbname)
                    exit(1)
                print u"Directory {0}{1}/{2}/{3} created".format(mount, i, host, dbname)
        
            if (j <= parallel):
                rmanfiled_b.write(u"allocate channel fs{0} device type disk format = '{1}{2}/{3}/{4}/%d_%T_%U.bdf';".format(j, mount, i, host, dbname))
                rmanfilea_b.write(u"allocate channel fs{0} device type disk format = '{1}{2}/{3}/{4}/%d_%T_%U.blf';" .format(j, mount, i, host, dbname))
                rmanfiled_e.write(u"release channel fs{0};".format(j))
                rmanfilea_e.write(u"release channel fs{0};".format(j))
            i+=1
            j+=1
        
            if (i> num and j <= parallel):
                i = 1
        else:
            print u"{0}{1} is not a mount point. Backup will not start".format(mount, i)
            print u"The mount prefix may not be correct or"
            print u"The input of the number of mount points may exceed the actuall number of mount points"
            exit(1)
    rmanfiled_b.write(u"backup INCREMENTAL LEVEL {0} CUMULATIVE database filesperset=1;".format(level))
    rmanfiled_b.write(u"sql 'alter system switch logfile';")
    if (archretday == 0 ):
        rmanfilea_b.write(u"backup archivelog all delete input;")
    else:
        rmanfilea_b.write(u"backup archivelog all archivelog until time 'sysdate-{0}' delete input;".format(archretday))

    #cat $rmanfiled_b $rmanfiled_e > $rmanfiled
    script = u"cat {0} {1} > {2}".format(rmanfiled_b.name, rmanfiled_e.name, rmanfiled.name)
    call(script, shell=True)
    #cat $rmanfilea_b $rmanfilea_e > $rmanfilea
    script = u"cat {0} {1} > {2}".format(rmanfilea_b.name, rmanfilea_e.name, rmanfilea.name)
    call(script, shell=True)

    rmanfiled.write(u"}")
    rmanfilea.write(u"}")
    rmanfiled.write(u"exit;")
    rmanfilea.write(u"exit;")

    runlog.write(u"finished creating rman file")
    print u"finished creating rman file"
    


# # create_rmanfile_archive
def create_rmanfile_archive(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e):
    runlog.write(u'Create rman file')
    rmanfilea_b.write(u"CONFIGURE DEFAULT DEVICE TYPE TO disk;")
    rmanfilea_b.write(u"CONFIGURE CONTROLFILE AUTOBACKUP OFF;")
    rmanfilea_b.write(u"   ")
    rmanfilea_b.write(u"RUN {")

    i=1
    j=1
    while i <= num:
        if os.path.ismount(u"{0}{1}".format(mount, i)):
            print u"{0}{1} is mount point".format(mount, i)
            print u" "
            if os.path.isdir(u"{0}{1}/{2}/{3}".format(mount, i, host, dbname)) == False:
                print u"Directory {0}{1}/{2}/{3} does not exist, create it".format(mount, i, host, dbname)
                try:
                    os.system(u"mkdir -p {0}{1}/{2}/{3}".format(mount, i, host, dbname))
                except OSError:
                    print u"Exception created with create new directory {0}{1}/{2}/{3}".format(mount, i, host, dbname)
                    exit(1)
                print u"Directory {0}{1}/{2}/{3} created".format(mount, i, host, dbname)
            if (j <= parallel):
                rmanfilea_b.write(u"allocate channel fs{0} device type disk format = '{1}{2}/{3}/{4}/%d_%T_%U.bdf';".format(j, mount, i, host, dbname))
                rmanfilea_e.write(u"release channel fs{0};".format(j))
            i+=1
            j+=1
            if (i> num and j <= parallel):
                i=1
        else:
            print u"{0}{1} is not a mount point. Backup will not start".format(mount, i)
            print u"The mount prefix may not be correct or"
            print u"The input of the number of mount points may exceed the actuall number of mount points"
            exit(1)
    if (archretday == 0):
        rmanfilea_b.write(u"backup archivelog all delete input;")
    else:
        rmanfilea_b.write(u"backup archivelog all archivelog until time 'sysdate-{0}' delete input;".format(archretday))

    #cat $rmanfilea_b $rmanfilea_e > $rmanfilea
    script = u"cat {0} {1} > {2}".format(rmanfilea_b.name, rmanfilea_e.name, rmanfilea.name)
    call(script, shell=True)
    rmanfilea.write(u"}")
    rmanfilea.write(u"exit;")

    runlog.write(u"finished creating rman file")
    print u"finished creating rman file"


# # archive
def archive():
    DATE_SUFFIX = datetime.now().strftime(u"%Y%m%d%H%M%S")
    print u"Archive logs backup started at `/bin/date{0}`".format(DATE_SUFFIX)
    runlog.write(u"Archive logs backup started at `/bin/date{0}`".format(DATE_SUFFIX))

    cmd = [u"{0}".format(rmanlogin),
           u"log", 
           u"{0}".format(rmanloga.name),
           u"@{0}".format(rmanfilea.name)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    result = o.decode(u'ascii')
    if(len(result.splitlines()) != 0 or e.decode(u'ascii') != 0):
        print u"Archive logs backup failed at `/bin/date{0}`".format(DATE_SUFFIX)
        runlog.write(u"Archive logs backup failed at `/bin/date{0}`".format(DATE_SUFFIX))
        print e.decode(u'ascii')
        for line in rmanloga.readline():
            print line
    else:
        print u"Archive logs backup finished at `/bin/date{0}`".format(DATE_SUFFIX)
        runlog.write(u"Archive logs backup finished at `/bin/date{0}`".format(DATE_SUFFIX))


# # backup
def backup():
    DATE_SUFFIX = datetime.now().strftime(u"%Y%m%d%H%M%S")
    print u"Database backup started at `/bin/date{0}`".format(DATE_SUFFIX)
    runlog.write(u"Database backup started at `/bin/date{0}`".format(DATE_SUFFIX))

    cmd = [u"{0}".format(rmanlogin),
           u"log", 
           u"{0}".format(rmanlog.name),
           u"@{0}".format(rmanfiled.name)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    result = o.decode(u'ascii')
    if(len(result.splitlines()) != 0 or e.decode(u'ascii') != 0):
        print u"Database backup failed at `/bin/date{0}`".format(DATE_SUFFIX)
        runlog.write(u"Databases backup failed at `/bin/date{0}`".format(DATE_SUFFIX))
        print e.decode(u'ascii')
        for line in rmanloga.readline():
            print line
    else:
        print u"Database backup finished at `/bin/date{0}`".format(DATE_SUFFIX)
        runlog.write(u"Database backup finished at `/bin/date{0}`".format(DATE_SUFFIX))


# # MAIN Function


runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e = setup()
if (archivelogonly):
    print u"archive logs backup only"
    create_rmanfile_archive(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e)
    archive
else:
    print u"backup database plus archive logs"
    create_rmanfile_all(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e)
    backup()
    archive()

retnewday=retday+1
DATE_SUFFIX = datetime.now().strftime(u"%Y%m%d%H%M%S")
runlog.write(u"Clean old backup longer than $retnewday started at `/bin/date{0}`".format(DATE_SUFFIX))
temp = u"{}"
output = subprocess.check_output(u"find {}1/{}/{} -type f -mtime +{} -exec /bin/rm -f {} \;".format(mount, host, dbname, retnewday, temp))
output = subprocess.check_output(u"find {}1/{}/{} -depth -type d -empty -exec rmdir {} \;".format(mount, host, dbname, temp))

cmd = [u"grep", u"-i", u"error", u"{0}".format(runlog.name)]
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
o, e = proc.communicate()
result = o.decode(u'ascii')
if(len(e.decode(u'ascii'))== 0):
    result = result.splitlines()
else:
    print u"Incorrect parsing of the error grep fir runlog"
if(len(result) != 0):
    print u"Backup is successful. However there are channels not correct"
    exit(1)

