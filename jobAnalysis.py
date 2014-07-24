import os, sys

class JobAnalysis():
    def __init__(self, simName, lsf='runall.lsf'): 
        self.cwd = os.getcwd()
        self.lsf = lsf
        self.simName = simName

    def getCurrDateTime(self):
        ''' Returns the date and time back in a format that is like
        m/d/y hh:mm'''
        import datetime 

        now = datetime.datetime.now() 
        now = [now.month, now.day, now.year, now.hour, now.minute]
        time = '%02d/%02d/%04d %02d:%02d' % tuple(now)

        return time


    def getRunallTime(self): 
        times = []
        with open(self.lsf) as f: 
            data_full = f.readlines()
            for data in data_full:
                 try:
                     if data.split()[1] == 'day' or data.split()[1]=='endday':
                         times.append(int(data.split()[-1].rstrip(';')))
                 except:
                     pass
            return times
    
    def getPBSInfo(self):
        PBS = []
        with open(self.lsf) as f: 
            data_full = f.readlines()
            for data in data_full:
                try:
                    if data.split()[0] == '#PBS':
                        PBS.append(data)
                except:
                    pass
        return PBS

    def getAprunInfo(self):
        aprun = []
        with open(self.lsf) as f:
            data_full = f.readlines()
            for data in data_full:
                try:
                    if data.split()[0].find('aprun') != -1:
                        aprun.append(data)
                except:
                    pass
        return aprun

    def getMSHViews(self): 
        with open('MSHviews') as f: 
            data = f.readlines()
        
        return data

    def getPreQCTxt(self): 
        txt = []
        os.chdir('PreQC')
        with open(simName+'.txt') as f:
            for line in f:
                txt.append(line)
        os.chdir('../')
        return txt

    def readMesh(fname): 
        twoDM = False
        threeDM = False
        ext = fname.split('.')[0]
        if ext == '2dm':
            twoDM = True
        elif ext == '3dm':
            threeDM = True
        else:
            print 'Invalid extension'
            exit()
        nx = []
        ny = []
        nz = []
        nnodes = 0 
        nfacets = 0 
        with open(fname) as f: 
            for line in f:
                if line.split()[0] == "E3T" and twoDM:
                    nfacets += 1 
                elif line.split()[0] == "E4T" and threeDM:
                    nfacets += 1
                elif line.split()[0] == "ND": 
                    nnodes += 1 
                    nx.append(float(line.split()[2]))
                    ny.append(float(line.split()[3]))
                    nz.append(float(line.split()[4]))
                else:
                    pass
        min_x = min(nx)
        max_x = max(nx) 
        min_y = min(ny)
        max_y = max(ny)
        min_z = min(nz)
        max_z = max(nz) 
        return (nnodes, nfacets, min_x, max_x, min_y, max_y, min_z, max_z)

    def printMeshStats(stats):
        nnodes, nfacets, min_x, max_x, min_y, max_y, max_y, min_z, max_z = stats
        ctr_x = (max_x-min_x)/2
        ctr_y = (max_y-min_y)/2
        ctr_z = (max_z-min_z)/2
        data_str = 'Number of nodes: %d\n' % nnodes
        data_str += 'Number of facets: %d\n' %nfacets
        data_str += 'Min x: %10.3f Max x: %10.3f Ctr x: %10.3f' % (min_x, max_x, ctr_x)
        data_str += 'Min y: %10.3f Max y: %10.3f Ctr y: %10.3f' % (min_y, max_y, ctr_y)
        data_str += 'Min z: %10.3f Max z: %10.3f Ctr z: %10.3f' % (min_z, max_z, ctr_z)
        return data_str
