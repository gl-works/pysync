#!/usr/bin/python
###############################################################################
#
# ftp synchronizor in python
#
# (c) HANBORQ Inc. 2013-04
#     Willis Gong
#
###############################################################################

import os,sys
from ftplib import FTP, error_perm, error_temp, error_reply
from multiprocessing import Process, Pool

serverName=None
userName=None
passWord=None

def get_list(remotePath, localPath, pattern='*'):
    jobs = []
    try:
        ftp = assure(None, remotePath)
        try:
            transferList = ftp.nlst(pattern)
            nlp = localPath if localPath.endswith(os.sep) else (localPath + os.sep)
            if not os.path.exists(nlp):
                os.makedirs(nlp)
            for fl in transferList:
                localFile = nlp + fl
                ftp = assure(ftp, remotePath)
                if not os.path.exists(localFile) or os.path.getsize(localFile) != ftp.size(fl):
                    jobs.append({"rpath":remotePath, "lpath":nlp, 'name': fl})
        finally:
            ftp.close()
    except (error_perm, error_temp, error_reply) as e:
        print  >> sys.stderr, timeStamp() +" %s" % e
    except Exception, e:
        print  >> sys.stderr, timeStamp() +" %s" % e
    return jobs

def worker_func(jobs):
    ftp = None
    for job in jobs:
        sys.stdout.flush()
        fileObj = open(job['lpath']+job['name'], 'wb')
        ftp = assure(ftp, job['rpath'])
        ftp.retrbinary('RETR ' + job['name'], fileObj.write)
        fileObj.close()
    if ftp is not None:
        ftp.close()
    return

def assure(ftp, remote=None):
    if ftp is not None:
        try:
            ftp.sendcmd('NOOP')
        except:
            ftp = None
    if ftp is None:
        ftp = FTP(serverName, userName, passWord)
    if remote is not None:
        ftp.cwd(remote)
    return ftp

def timeStamp():
    import time
    return str(time.strftime("%a %d %b %Y %I:%M:%S %p"))

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
             for i in range(wanted_parts) ]

def initialize(s, u, p):
    global serverName
    global userName
    global passWord
    serverName = s
    userName = u
    passWord = p

def main(argv):
    if len(argv) < 6:
        print  >> sys.stderr, "Usage: %s server user passwd remotedir localdir [pattern [parallel]]" % os.path.split(argv[0])[1]
        return
    initialize(argv[1], argv[2], argv[3])
    rpath = argv[4]
    lpath = argv[5]
    pattern = argv[6] if len(argv) > 6 else '*'
    parallel = int(argv[7] if len(argv) > 7 else "2")
    jobs = get_list(rpath, lpath, pattern)
    pool = Pool(processes=parallel)
    for split in split_list(jobs, parallel):
        pool.apply_async(worker_func, (split, ))
    pool.close()
    pool.join()
    return

if __name__ == '__main__':
    main(sys.argv)
    pass