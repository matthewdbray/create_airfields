#!/usr/bin/env python 

import changemat
from collections import OrderedDict
import ConfigParser
import datetime
import os 
import meshdb
import monitorjobs
from multiprocessing import Pool 
import shutil
import sqlite3 as lite 
from subprocess import Popen, PIPE
import sys
from time import sleep, asctime, localtime 

class JobProperties(object):
    ' Common class for holding job properties much like a struct '

    def __init__(self, jobsdb, hashid):
        self.jobsdb = jobsdb
        self.props = OrderedDict()
        self.hashid = hashid
        self.createHashTable()

    def createHashTable(self):
        try:
            con = lite.connect(self.jobsdb)
            cur = con.cursor()
        except:
            print("Unable to find the database.  Please be in the sim_dir and "
                    "populate the db folder with the proper databases\n")
            print "Tried to open jobs database:", self.jobsdb
            sys.exit()
        with con:
            # Getting table names for jobs
            cmd = 'PRAGMA table_info(Jobs)'
            pragma_data = cur.execute(cmd).fetchall()
            columns = [p[1] for p in pragma_data]

            # Getting information from particular job 
            cmd = 'SELECT * FROM Jobs WHERE HASHID = "%s"' % self.hashid
            job_info = cur.execute(cmd).fetchall()[0]

        # Creating the hash table
        for i, column in enumerate(columns):
            self.props[column] = job_info[i]

    def getProp(self, prop):
        return self.props[prop]

    def setProp(self, prop, newValue):
        self.props[prop] = newValue

    def keys(self):
        for k in self.props.iterkeys():
            return k 
    
    def isIn(self, line):
        isIn = False
        line = line.split()
        for k in self.props.iterkeys():
            kref = '<'+k+'>'   # exmaple in code would like like <soil_pav>
            for w in line:
                if kref.upper() == w.upper():
                    isIn = True
                    break
            if isIn: 
                break
        return isIn, kref.lower(), k

    def __str__(self):
        s = ""
        for k,v in self.props.iteritems():
            s += "key: %s\tvalue: %s\n" % (k,v)
        return s

class JobSpawner(object):
    ' Common class for spawning jobs and job control ' 

    def __init__(self, jobsdb, mshdb, cnf, sim_name): 
        self.jobsdb = jobsdb
        self.mshdb = mshdb 
        self.cnf = cnf
        self.sim_name = sim_name
        self.nextjob = ""
        self.numjobsrunning = 0
        self.totconjobs = 3  # This is hardcoded for the time being
        self.jobsrunning = []
        self.totaljobsran = 0
        self.simdir = os.getcwd() + '/'

    def createHotStart(self, nodes, facets):
        # Create hotstart file 
        props = JobProperties(self.jobsdb, self.nextjob)
        head = props.getProp('HEAD') 
        bot_ts = props.getProp('BOT_TS') 
        header='DATASET\nOBJTYPE "mesh3d"\nBEGSCL\n'                                                               
        header +='ND %s\n' % nodes                                       
        header += 'NC %s\n' % facets                                         
        ih_header = header + 'NAME "IH"\nTS 0 0\n'         
        tmp_header = header + 'NAME "IT"\nTS 0 0\n'
        with open('jobs/'+self.nextjob+'/'+self.nextjob+'.hot','w') as outfile:
            outfile.write(ih_header)
            for i in range(int(nodes)):
                outfile.write('%s\n' % head)
            outfile.write('ENDSS\n')
            outfile.write(tmp_header)
            for i in range(int(nodes)):
                outfile.write('%s\n' % bot_ts) 
            outfile.write('ENDDS')
        print "Hotstart file created with %d IH and %d IT" % (head, bot_ts)

    def createBcFile(self, bcs):
        try:
            config = ConfigParser.ConfigParser()
            config.read(self.cnf)

        except:
            print "Config file not found in config directory."
            sys.exit()

        # Getting the start and end time from the config file
        st = config.get('sim_info','start_time')
        st = st.split()[0]
        stime = datetime.datetime.strptime(st, '%Y-%m-%d')
        start_time = stime.timetuple().tm_yday 
        et = config.get('sim_info','end_time')
        et = et.split()[0]
        etime = datetime.datetime.strptime(et, '%Y-%m-%d')
        end_time = etime.timetuple().tm_yday

        # Getting the properties from the jobs.db
        props = JobProperties(self.jobsdb, self.nextjob)
        props.setProp('start_day',start_time)
        props.setProp('end_hour', (end_time-start_time)*24+1)
        bc_handle = config.get('sim_info','bc_template')
        new_bc = []
        with open(bc_handle) as infile:
            for line in infile.readlines(): 
                inLine, propref, prop = props.isIn(line)
                if inLine:
                    line = line.split()
                    index = line.index(propref)
                    line[index] = str(props.getProp(prop))
                    line = ' '.join(line)
                    line += '\n'
                    new_bc.append(line)
                else:
                    new_bc.append(line)

        with open('jobs/'+self.nextjob+'/'+self.nextjob+'.bc','w') as outfile:
            outfile.write('! '+self.nextjob+' simulation bc file\n')
            for line in new_bc: 
                outfile.write(line)
            outfile.write('\n'+bcs+'END')
        print "BC File created %s" % self.nextjob+'.bc'


    def getNextJob(self):

        try:
            self.con = lite.connect(self.jobsdb)
            cur = self.con.cursor()
        except:
            print("Unable to find the database.  Please be in sim_dir and " 
                    "populate the db folder with the databases") 
            print "Tried to open database:",self.jobsdb
            sys.exit()

        with self.con:
            cmd = 'SELECT running, complete, hashid FROM Jobs WHERE\
                        sim_name = "%s"' %  self.sim_name
            jobs = cur.execute(cmd).fetchall()
            # Separating out the lists in jobs into their respective lists
            running, comp, hashid = \
                    zip(*[(x[0],x[1],x[2]) for x in jobs])

            foundJob = False
            totaljobs = len(running)
            job_count = 1
            for r,c,h in zip(running, comp, hashid):
#                print r, h 
                if r == "N" and c == "N": 
                    foundJob = True
                    if foundJob and self.numjobsrunning < self.totconjobs:
                        self.nextjob = str(h)  # Convert from unicode
                        self.updateJobsDB('RUNNING','Y')
                        print "Next job to run is %s" % self.nextjob
                        break
                if job_count==totaljobs and not foundJob:
                    print "There are no more jobs to complete"
                    sys.exit()
                job_count += 1 

        return self.nextjob

    def jobSetup(self, hashid=None):
        """
        This sets up all the necessary files in the simdir/jobs directory.

        If you do not feed it a hashid then it will choose self.nextjob.  This
        is set to do both so there is no confusion when running in parallel.
        """
        # Seeing if it is running in parallel or serially
        if hashid is None:
            hashid = self.nextjob 
        # Testing for necessary directories in simdir
        cwd = os.getcwd() 
        os.chdir(cwd) 
        necDirs = True
        necessary_dirs = ['db','config','bin'] # We could add to this list later
        for directory in necessary_dirs:
            if os.path.exists(directory) is False:
                print "You are missing the %s directory in your simdir\n"\
                        % directory
                necDirs = False
            if not necDirs:
                sys.exit()
        
        # Set up the directory
        if not os.path.exists('jobs'):
            os.mkdir('jobs', 0775)
        if not os.path.exists('jobs/'+ hashid):
            os.mkdir('jobs/'+ hashid, 0775)

        # Get mesh ID 
        try:
            self.con = lite.connect(self.jobsdb)
            cur = self.con.cursor()
        except:
            print "Make sure you have a %s in your db/ directory" % self.jobsdb
            sys.exit()
        with self.con:
            try: 
                cmd = 'SELECT PAV_DZ, BASE_DZ, SOIL_DZ, MESH_ID from Jobs where\
                                hashid = "%s"' % hashid 
            except:
                cmd = 'SELECT PAV_DZ, BASE_DZ, SOIL_DZ, MESH_ID from Jobs\
                        LIMIT 1'

            stats = cur.execute(cmd).fetchall()
            pav_dz, base_dz, soil_dz, meshid = stats[0]
            tot_z = pav_dz + base_dz + soil_dz
        
            print "pav_dz: " + str(pav_dz)
            print "base_dz: " + str(base_dz)
        
        # Get the mesh and bc cards
        msd = meshdb.MeshDB(self.mshdb)
        mesh_name, mesh, bcs = msd.get(meshid)
        mesh = "%s" % mesh 
        bcs = "%s" % bcs
        with open('jobs/%s/%s.3dm' % (hashid, hashid),'w') as f:
            f.write(mesh+'END')
        #with open('/jobs/%s/%s.bcs' % (nextjob, nextjob)) as f:
            #f.write(bcs)
        
        # Change the materials
        os.chdir('jobs/'+hashid)
        changemat.changeMaterials(mesh_name = hashid +'.3dm',\
                depth=[pav_dz, base_dz])
        os.chdir(self.simdir)

        # Create bc file 
        self.createBcFile(bcs)

        # Create hotstart file 
        nodes, facets = msd.getNdsAndFcs(meshid)
        self.createHotStart(nodes, facets)

        # Grab the met file 
        try:
            config = ConfigParser.ConfigParser()
            config.read(self.cnf)
        except:
            print "Config file not found in config directory."
            sys.exit()
        met_path = config.get('sim_info','met_file')
        met_handle = met_path.split('/')[-1]

        if not os.path.isfile('config/'+met_handle):
            try:
                shutil.copyfile(met_path, 'config/')
                print "Using the met from %s" % met_path
            except:
                print "No met file found"
                sys.exit()
        else:
            shutil.copyfile('config/'+met_handle,'jobs/'+ hashid +'/'+\
                    hashid +'.met')
            print "Using the %s met file from the config folder" % met_handle

    def updateJobsDB(self, toset, value, hashid=None):
        """ 
        This sets the jobs.db you are working with because I was doing it 
        over and over in the code and wanted to stop having to copy and
        paste.  You feed it the name of the key you want to change and the 
        value you want to change it to
        
        By default it is going to use the self.nextjob, but in a parallel 
        environment this might have changed since the job finished so you'll 
        need to supply it with the actual hashid 

        Serial
        input: value in database to set, new value 
        Parallel
        input: value in database to set, new value, hashid
        """
        # For serial job
        if hashid is None: 
            hashid = self.nextjob

        #======================================================================
        # Trying to connect to the database
        #======================================================================
        try:
            self.con = lite.connect(self.simdir+self.jobsdb)
            cur = self.con.cursor()
        except:
            print("Unable to find the database.  Please be in sim_dir and " 
                    "populate the db folder with the databases") 
            print "Tried to open database:",self.jobsdb
            sys.exit()
        with self.con:
            cmd = 'UPDATE Jobs SET %s = "%s" WHERE hashid = "%s"'\
                    % (toset, value, hashid)
            cur.execute(cmd)

    def runSerially(self):
        self.getNextJob()
        self.jobSetup()
        print "Running job %s" % self.nextjob

        os.chdir('jobs/'+self.nextjob)
        print "Job starting at %s" % asctime(localtime())
        logfile = open(self.nextjob+'.log','w')
        errfile = open(self.nextjob+'.err', 'w')
        p1 = Popen(['../../bin/adh', self.nextjob],stdout=logfile, stderr=errfile) 
        #output = p1.communicate()[0]
        self.updateJobsDB('PID', p1.pid)
        p1.wait()
        print "Job %s has finished at %s" % (self.nextjob, asctime(localtime()))
        #print stdout
        #with open(self.nextjob+'.log','w') as outfile:
            #outfile.write(output)
        print "Simulation %s has finished." % self.nextjob

        #  move back up
        os.chdir(self.simdir)
        self.numjobsrunning -= 1 

        self.updateJobsDB('COMPLETE', 'Y')

    def runJobParallel(self, cmd):
        """
        This is fed a cmd after being unwrapped by an unwrapping function 
        outside of the code that strips self for pickling reasons.  Google
        pickling classes multiprocessing for more info on why. 

        This runs the jobs in parallel, and returns the exitcode or an error
        """ 
        os.chdir(self.simdir+'jobs/'+cmd[-1]) # Should give me job
        try:
            with open(cmd[-1]+'.log','w') as logfile:
                p1 = Popen(cmd, stdout=logfile)
            os.chdir(self.simdir+'jobs/')
            return cmd, p1.wait(), None # Use this to find if error is None
        except Exception as e:
            os.chdir(self.simdir+'jobs/')
            return cmd, None, str(e)


    def runJobsParallel(self):
        """ 
        This will run the jobs concurrently with however many number of
        processors you give it.  
        
        This will be set manually with a mac and will probably end up reading
        the PBS information on the supercomputer.
        """
        
        self.maxprocs = 3  # This will be set somewhere else eventually
#        self.numprocs = 0 
        #======================================================================
        # Set the size of the pool that will spawn the jobs concurrently.
        #======================================================================
        pool = Pool(self.maxprocs) 
        #======================================================================
        # Set up the intial first jobs to spawn.  I imagine we'll want to grab
        # the first jobs that haven't been ran at the database at this point.
        # Later after these runs complete we might choose to do some scoring
        # methods, so I'm letting another part of the code handle that.
        #======================================================================
        cmds = []
        for i in range(self.maxprocs):
            print i 
            hashid = self.getNextJob()
            self.jobSetup(hashid)
            print hashid + " this is the next job" 
            cmds.append(['../../bin/adh', hashid ]) 

        #======================================================================
        # This will look to see when a job finishes and add another one to the
        # cmds list, if not it should just wait until a job finishes and add
        # one.  I've commented lines out for updating the ERROR in the DB for
        # now. 
        #======================================================================
        while True:
            for cmd, status, error in pool.imap_unordered(unwrap_self,zip([self]*len(cmds), cmds)):
                if error is None: 
                    print "%s job has completed with status %s" % (cmd, status)
                    cmds.remove(cmd)
                    print "%s has been removed" % cmd
                    self.updateJobsDB('COMPLETE','Y', hashid=hashid)
                    # self.updateJobsDB('ERROR','N', hashid=hashid)
                    hashid = self.getNextJob()
                    self.jobSetup(hashid)
                    cmds.append(['../../bin/adh', hashid])
                else:
                    print "%s job failed with error %s" % (cmd, error)
                    cmds.remove(cmd)
                    # self.updateJobsDB('ERROR','Y',hashid=cmd[-1])
                    hashid = self.getNextJob()
                    self.jobSetup(hashid)
                    cmds.append(['../../bin/adh', hashid])
            
            #==================================================================
            # This is like a wait which hopefully keeps the last jobs from not
            # being completed after the first job of that pool is done.
            #==================================================================
            pool.join() 
            

def unwrap_self(arg, **kwargs):
    """ 
    This is simply to unwrap the Pool() call to it's intended target.  This 
    has to do with a pickling issue within Classes in python.
    """ 

    return JobSpawner.runJobParallel(*arg, **kwargs)


if __name__ == '__main__': 
    
    cwd = os.getcwd()
    os.chdir(cwd)
    jobsdb = 'db/jobs.db'
    #hashid = '59b8da00c8fd1a7f5a9b065ea235c5df' 
    mshdb = 'db/meshes.db'
    cnf = 'hagler.ini'
    sim_name = 'hagler'
    job = JobSpawner(jobsdb, mshdb, cnf, sim_name)
    job.runJobsParallel()
        #job.runSerially()

