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
def show_usage():
    print("usage: python backup-ora-coh-nfs.py [--help] -r <RMAN login> -h <host> -o <Oracle_sid> -a <archive only> -i <incremental level> -m <mount-prefix> -n <number of mounts> -p <number of channels> -e <retention> -l <archive log keep days>") 
    print(" -r : RMAN login (example: \"rman target /\", optional)")
    print(" -h : host (optional)")
    print(" -o : ORACLE_SID" )
    print(" -a : arch (yes means archivelogonly, no means database backup plus archivelog)")
    print(" -i : Incremental level")
    print(" -m : mount-prefix (like /mnt/ora)")
    print(" -n : number of mounts")
    print(" -p : number of channels (Optional, default is 4)")
    print(" -e : Retention time (days to retain the backups)")
    print(" -l : Archive logs retain days (days to retain the local archivelogs before deleting them. default is 1 day)")


# # Imports
import re
import os
import sys
import argparse
import subprocess
from subprocess import call
from datetime import datetime


# # Command Line Argument Parsing
rmanlogin = ''
host = ''
dbname = ''
arch = False
level = -1
mount = ''
num = -1
parallel = 4
retday = -1
archretday = 1

archivelogonly = False

argv = sys.argv[1:]
argv = [i.lower() for i in argv]
if '--help' in argv or 'help' in argv or '-help' in argv:
    show_usage()
    print("exit")
    exit(0)
parser = argparse.ArgumentParser(description = "Parser for backup-ora-coh-nfs", conflict_handler='resolve')

parser.add_argument("-r", "--rmanlogin", help = "RMAN login (example: \"rman target /\", optional)", 
                    required = False, default = "")
parser.add_argument("-h", "--host", help = "host (optional)", 
                    required = False, default = "")
parser.add_argument("-o", "--dbname", help = "ORACLE_SID", 
                    required = True, default = "")
parser.add_argument("-a", "--arch", help = "arch (yes means archivelogonly, no means database backup plus archivelog)", 
                    required = True, default = "")
parser.add_argument("-i", "--level", help = "increment level",
                    required = False, default = "0")
parser.add_argument("-m", "--mount", help = "mount-prefix (like /mnt/ora)",
    					required = True, default = "")
parser.add_argument("-n", "--num", help = "number of mounts",
    					required = True, default = "")
parser.add_argument("-p", "--parallel", help = "number of channels (Optional, default is 4)",
    					required = False, default = "4")
parser.add_argument("-e", "--retday", help = "Retention time (days to retain the backups)",
    					required = True, default= "")
parser.add_argument("-l", "--archretday", help = "Archive logs retain days (days to retain the local archivelogs before deleting them. default is 1 day)",
    					required = False, default= "1")

# Check required parameters
args = parser.parse_args(argv)

rmanlogin = args.rmanlogin
host = args.host
if host == '':
    host = "hostname -s"

if rmanlogin == '':
    rmanlogin = "rman target /"
dbname = args.dbname
mount = args.mount
try:
    num = int(args.num)
except ValueError:
    print("Invalid input for number of mounts")
    show_usage()
    exit(1)
if (num < 1):
    print("Invalid input for number of mounts")
    show_usage()
    exit(1)
    
try:
    parallel = int(args.parallel)
except ValueError:
    print("Invalid input for number of channels")
    show_usage()
    exit(1)
if (parallel < 1):
    print("Invalid input for number of channels")
    show_usage()
    exit(1)
        
try:
    retday = int(args.retday)
except ValueError:
    print("Invalid input for retention time")
    show_usage()
    exit(1)
if (retday < 1):
    print("Invalid input for retention time")
    show_usage()
    exit(1)
    
print(args.arch.lower())
if (args.arch.lower() == "no" or args.arch.lower() == "arch"):
    print("only backup archive logs")
    archivelogonly = True
else:
    print("will backup database backup plus archive logs")
    try:
        level = int(args.level)
    except ValueError:
        print("incremental level is set to be {0}. Backup won't start" .format(args.level))
        print("incremental backup level needs to be either 0 or 1\n")
        show_usage()
        exit(1)
    if (level != 0 and level != 1):
        print("incremental level is set to be {0}. Backup won't start" .format(args.level))
        print("incremental backup level needs to be either 0 or 1\n")
        show_usage()
        exit(1)
            
    try:
        archretday = int(args.archretday)
    except ValueError:
        print("archive retention day is set to be {0}. Backup won't start" .format(args.archretday))
        print("please enter a positive integer for the archive retention day")
        show_usage()
        exit(1)
    if (archretday < 1):
        print("archive retention day is set to be {0}. Backup won't start" .format(args.archretday))
        print("please enter a positive integer for the archive retention day")
        show_usage()
        exit(1)


# # Set Up
def setup():
    print("rmanlogin is \{0}\"".format(rmanlogin))
    print("rmanlogin syntax can be like \"rman target /\" or ")
    print("\"rman target sys/<password>@<database connect string> catalog <user>/<password>@<catalog>\"")
    DATE_SUFFIX = datetime.now().strftime("%Y%m%d%H%M%S")
    cwd = os.getcwd()
    if os.path.isdir(cwd+"/log/{0}".format(host)) == False:
        print("{0}/log/{1} does not exist, create it".format(cwd, host))
        try:
            os.system("mkdir -p {0}/log/{1}".format(cwd, host))
        except OSError:
            print("Exception created with create new directory ./log")
            exit(1)
    
    runlog = '{0}/log/{1}/{2}.{3}.log'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(runlog):
        os.system('touch {0}'.format(runlog))
        print(runlog)
    runlog = open(runlog, "a")

    rmanlog = '{0}/log/{1}/{2}.rman.{3}.log'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanlog):
        os.system('touch {0}'.format(rmanlog))
        print(rmanlog)
    rmanlog = open(rmanlog, "a")

    rmanloga = '{0}/log/{1}/{2}.archive.{3}.log'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanloga):
        os.system('touch {0}'.format(rmanloga))
        print(rmanloga)
    rmanloga = open(rmanloga, "a")

    rmanfiled = '{0}/log/{1}/{2}.rman.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfiled):
        os.system('touch {0}'.format(rmanfiled))
        print(rmanfiled)
    rmanfiled = open(rmanfiled, "a")

    rmanfiled_b = '{0}/log/{1}/{2}.rman_b.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfiled_b):
        os.system('touch {0}'.format(rmanfiled_b))
        print(rmanfiled_b)
    rmanfiled_b = open(rmanfiled_b, "a")

    rmanfiled_e = '{0}/log/{1}/{2}.rman_e.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfiled_e):
        os.system('touch {0}'.format(rmanfiled_e))
        print(rmanfiled_e)
    rmanfiled_e = open(rmanfiled_e, "a")

    rmanfilea = '{0}/log/{1}/{2}.archive.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfilea):
        os.system('touch {0}'.format(rmanfilea))
        print(rmanfilea)
    rmanfilea = open(rmanfilea, "a")

    rmanfilea_b = '{0}/log/{1}/{2}.archive_b.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfilea_b):
        os.system('touch {0}'.format(rmanfilea_b))
        print(rmanfilea_b)
    rmanfilea_b = open(rmanfilea_b, "a")

    rmanfilea_e = '{0}/log/{1}/{2}.archive_e.{3}.crx'.format(cwd, host, dbname, DATE_SUFFIX)
    if not os.path.exists(rmanfilea_e):
        os.system('touch {0}'.format(rmanfilea_e))
        print(rmanfilea_e)
    rmanfilea_e = open(rmanfilea_e, "a")
    
    #trim log directory
    temp = "{}"
    cmd = "find {}/log/{} -type f -mtime +7 -exec /bin/rm {} \;".format(cwd, host, temp)
    output = subprocess.check_output(cmd)

    if (len(output) != 0):
        runlog.write("del old logs in {}/log/{} failed".format(cwd, host))
        print("del old logs in {}/log/{} failed".format(cwd, host))
        exit(2)
    
    ##ps -ef
    print("check whether this database is up running")
    cmd = ["ps", "-ef"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    output = o.decode('ascii')
    if(len(e.decode('ascii'))== 0):
        output = output.splitlines()
    else:
        print(e)
        print("Error when running ps -ef")
        exit(2)
    
    result = []
    ##grep pmon | awk '{print $8}'
    for line in output:
        if re.search("pmon", line):
            result.append(line.split( )[8])
    arroid = []      
    ##grep -i $dbname | awk -F "pmon" 'print {$2}'
    for line in result:
        if re.search("{0}".format(dbname), line):
            arroid.append(line.split("pmon")[2])
    up = False
    print(dbname)
    yes_oracle_sid = ""
    
    for i in arroid:
        oracle_sid = i
        oracle_sid = oracle_sid[1:]
        print(oracle_sid)
        lastc = oracle_sid[-1]
        print(lastc)
        if (oracle_sid == dbname):
            print("Oracle database {0} is up on {1}. Backup can start".format(dbname, host))
            yes_orcale_sid = dbname
            up = True
        elif (re.search(r"^[0-9]+$", lastc)):
            if(oracle_sid[:-1] == dbname):
                print("Oracle database {0} is up on {1}. Backup can start".format(dbname, host))
                yes_oracle_sid=oracle_sid
                up = True
    if (up == False):
        print("Oracle database {0} is not up. Backup cannot start".format(dbname))
        exit(2)
    print("get ORACLE_HOME")
    cmd = ["grep", "-i", "{0}".format(yes_oracle_sid), '/etc/oratab']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    oratabinfo = o.decode('ascii')
    if(len(e.decode('ascii'))== 0):
        oratabinfo = oratabinfo.splitlines()
    else:
        print(e)
        print("Error when running ps -ef")
        exit(2)
    if oratabinfo == "" or len(oratabinfo) == 0:
        print("No Oracle sid {0} information in /etc/oratab. Cannot determine ORACLE_HOME".format(yes_oracle_sid))
        exit(2)
    k = 0
    arrinfo = oratabinfo
    oracle_home = ""
    for i in range(len(oratabinfo)):
        #`echo ${arrinfo[$i]} | awk -F ":" '{print $1}'`
        orasidintab = append(arrinfo[i].split(':')[1])
        #`echo ${arrinfo[$i]} | awk -F ":" '{print $2}'`
        orahomeintab=append(arrinfo[i].split(':')[2])
        if (orasidintab[0] == yes_oracle_sid):    
            oracle_home=orahomeintab
            #export ORACLE_HOME=$oracle_home
            os.environ["ORACLE_HOME"] = oracle_home
            #export PATH=$PATH:$ORACLE_HOME/bin
            os.environ["PATH"] = "PATH:{0}/bin".format(oracle_home)
            k=1
        print("orasidintab is {0}". format(orasidintab))
    if (k ==0):
        print("No Oracle sid {0} information in /etc/oratab. Cannot determine ORACLE_HOME".format(dbname))
        exit(2)
    else:
        print("ORACLE_HOME is {0}".format(ORACLE_HOME))
    os.environ["ORACLE_SID"] = yes_oracle_sid
    return runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e


# # Create_rmanfile_all
def create_rmanfile_all(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e):
    runlog.write('Create rman file')
    rmanfiled_b.write('CONFIGURE DEFAULT DEVICE TYPE TO disk;')
    rmanfilea_b.write('CONFIGURE DEFAULT DEVICE TYPE TO disk;')
    rmanfiled_b.write('CONFIGURE CONTROLFILE AUTOBACKUP ON;')
    rmanfilea_b.write('CONFIGURE CONTROLFILE AUTOBACKUP OFF;')
    rmanfiled_b.write('CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO \'{{0}}1/{1}/{2}/%d_%F.ctl\';'.format(mount, host, dbname))
    rmanfiled_b.write('   ')
    rmanfilea_b.write('   ')
    rmanfiled_b.write('RUN {')
    rmanfilea_b.write('RUN {')

    i=1
    j=1
    while i <= num:
        if os.path.ismount("{0}{1}".format(mount, i)):
            print("{0}{1} is mount point".format(mount, i))
            print(" ")
            if os.path.isdir("{0}{1}/{2}/{3}".format(mount, i, host, dbname)) == False:
                print("Directory {0}{1}/{2}/{3} does not exist, create it".format(mount, i, host, dbname))
                try:
                    os.system("mkdir -p {0}{1}/{2}/{3}".format(mount, i, host, dbname))
                except OSError:
                    print("Exception created with create new directory {0}{1}/{2}/{3}".format(mount, i, host, dbname))
                    exit(1)
                print("Directory {0}{1}/{2}/{3} created".format(mount, i, host, dbname))
        
            if (j <= parallel):
                rmanfiled_b.write("allocate channel fs{0} device type disk format = '{1}{2}/{3}/{4}/%d_%T_%U.bdf';".format(j, mount, i, host, dbname))
                rmanfilea_b.write("allocate channel fs{0} device type disk format = '{1}{2}/{3}/{4}/%d_%T_%U.blf';" .format(j, mount, i, host, dbname))
                rmanfiled_e.write("release channel fs{0};".format(j))
                rmanfilea_e.write("release channel fs{0};".format(j))
            i+=1
            j+=1
        
            if (i> num and j <= parallel):
                i = 1
        else:
            print("{0}{1} is not a mount point. Backup will not start".format(mount, i))
            print("The mount prefix may not be correct or")
            print("The input of the number of mount points may exceed the actuall number of mount points")
            exit(1)
    rmanfiled_b.write("backup INCREMENTAL LEVEL {0} CUMULATIVE database filesperset=1;".format(level))
    rmanfiled_b.write("sql 'alter system switch logfile';")
    if (archretday == 0 ):
        rmanfilea_b.write("backup archivelog all delete input;")
    else:
        rmanfilea_b.write("backup archivelog all archivelog until time 'sysdate-{0}' delete input;".format(archretday))

    #cat $rmanfiled_b $rmanfiled_e > $rmanfiled
    script = "cat {0} {1} > {2}".format(rmanfiled_b.name, rmanfiled_e.name, rmanfiled.name)
    call(script, shell=True)
    #cat $rmanfilea_b $rmanfilea_e > $rmanfilea
    script = "cat {0} {1} > {2}".format(rmanfilea_b.name, rmanfilea_e.name, rmanfilea.name)
    call(script, shell=True)

    rmanfiled.write("}")
    rmanfilea.write("}")
    rmanfiled.write("exit;")
    rmanfilea.write("exit;")

    runlog.write("finished creating rman file")
    print("finished creating rman file")
    


# # create_rmanfile_archive
def create_rmanfile_archive(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e):
    runlog.write('Create rman file')
    rmanfilea_b.write("CONFIGURE DEFAULT DEVICE TYPE TO disk;")
    rmanfilea_b.write("CONFIGURE CONTROLFILE AUTOBACKUP OFF;")
    rmanfilea_b.write("   ")
    rmanfilea_b.write("RUN {")

    i=1
    j=1
    while i <= num:
        if os.path.ismount("{0}{1}".format(mount, i)):
            print("{0}{1} is mount point".format(mount, i))
            print(" ")
            if os.path.isdir("{0}{1}/{2}/{3}".format(mount, i, host, dbname)) == False:
                print("Directory {0}{1}/{2}/{3} does not exist, create it".format(mount, i, host, dbname))
                try:
                    os.system("mkdir -p {0}{1}/{2}/{3}".format(mount, i, host, dbname))
                except OSError:
                    print("Exception created with create new directory {0}{1}/{2}/{3}".format(mount, i, host, dbname))
                    exit(1)
                print("Directory {0}{1}/{2}/{3} created".format(mount, i, host, dbname))
            if (j <= parallel):
                rmanfilea_b.write("allocate channel fs{0} device type disk format = '{1}{2}/{3}/{4}/%d_%T_%U.bdf';".format(j, mount, i, host, dbname))
                rmanfilea_e.write("release channel fs{0};".format(j))
            i+=1
            j+=1
            if (i> num and j <= parallel):
                i=1
        else:
            print("{0}{1} is not a mount point. Backup will not start".format(mount, i))
            print("The mount prefix may not be correct or")
            print("The input of the number of mount points may exceed the actuall number of mount points")
            exit(1)
    if (archretday == 0):
        rmanfilea_b.write("backup archivelog all delete input;")
    else:
        rmanfilea_b.write("backup archivelog all archivelog until time 'sysdate-{0}' delete input;".format(archretday))

    #cat $rmanfilea_b $rmanfilea_e > $rmanfilea
    script = "cat {0} {1} > {2}".format(rmanfilea_b.name, rmanfilea_e.name, rmanfilea.name)
    call(script, shell=True)
    rmanfilea.write("}")
    rmanfilea.write("exit;")

    runlog.write("finished creating rman file")
    print("finished creating rman file")


# # archive
def archive():
    DATE_SUFFIX = datetime.now().strftime("%Y%m%d%H%M%S")
    print("Archive logs backup started at `/bin/date{0}`".format(DATE_SUFFIX))
    runlog.write("Archive logs backup started at `/bin/date{0}`".format(DATE_SUFFIX))

    cmd = ["{0}".format(rmanlogin),
           "log", 
           "{0}".format(rmanloga.name),
           "@{0}".format(rmanfilea.name)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    result = o.decode('ascii')
    if(len(result.splitlines()) != 0 or e.decode('ascii') != 0):
        print("Archive logs backup failed at `/bin/date{0}`".format(DATE_SUFFIX))
        runlog.write("Archive logs backup failed at `/bin/date{0}`".format(DATE_SUFFIX))
        print(e.decode('ascii'))
        for line in rmanloga.readline():
            print(line)
    else:
        print("Archive logs backup finished at `/bin/date{0}`".format(DATE_SUFFIX))
        runlog.write("Archive logs backup finished at `/bin/date{0}`".format(DATE_SUFFIX))


# # backup
def backup():
    DATE_SUFFIX = datetime.now().strftime("%Y%m%d%H%M%S")
    print("Database backup started at `/bin/date{0}`".format(DATE_SUFFIX))
    runlog.write("Database backup started at `/bin/date{0}`".format(DATE_SUFFIX))

    cmd = ["{0}".format(rmanlogin),
           "log", 
           "{0}".format(rmanlog.name),
           "@{0}".format(rmanfiled.name)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o, e = proc.communicate()
    result = o.decode('ascii')
    if(len(result.splitlines()) != 0 or e.decode('ascii') != 0):
        print("Database backup failed at `/bin/date{0}`".format(DATE_SUFFIX))
        runlog.write("Databases backup failed at `/bin/date{0}`".format(DATE_SUFFIX))
        print(e.decode('ascii'))
        for line in rmanloga.readline():
            print(line)
    else:
        print("Database backup finished at `/bin/date{0}`".format(DATE_SUFFIX))
        runlog.write("Database backup finished at `/bin/date{0}`".format(DATE_SUFFIX))


# # MAIN Function


runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e = setup()
if (archivelogonly):
    print("archive logs backup only")
    create_rmanfile_archive(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e)
    archive
else:
    print("backup database plus archive logs")
    create_rmanfile_all(runlog, rmanlog, rmanloga, rmanfiled, rmanfiled_b, rmanfiled_e, rmanfilea, rmanfilea_b, rmanfilea_e)
    backup()
    archive()

retnewday=retday+1
DATE_SUFFIX = datetime.now().strftime("%Y%m%d%H%M%S")
runlog.write("Clean old backup longer than $retnewday started at `/bin/date{0}`".format(DATE_SUFFIX))
temp = "{}"
output = subprocess.check_output("find {}1/{}/{} -type f -mtime +{} -exec /bin/rm -f {} \;".format(mount, host, dbname, retnewday, temp))
output = subprocess.check_output("find {}1/{}/{} -depth -type d -empty -exec rmdir {} \;".format(mount, host, dbname, temp))

cmd = ["grep", "-i", "error", "{0}".format(runlog.name)]
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
o, e = proc.communicate()
result = o.decode('ascii')
if(len(e.decode('ascii'))== 0):
    result = result.splitlines()
else:
    print("Incorrect parsing of the error grep fir runlog")
if(len(result) != 0):
    print("Backup is successful. However there are channels not correct")
    exit(1)

