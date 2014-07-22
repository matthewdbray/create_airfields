import os, sys

class JobAnalysis():
    def __init__(self, lsf='runall.lsf',simName): 
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

                


