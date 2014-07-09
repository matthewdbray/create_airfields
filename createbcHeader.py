#!/usr/bin/env python

import sys, os, glob
import xlrd
import numpy as np 

def print_usage(): 
    print "Usage: run this in the folder where you have your excel material files."
    print "Search for ??? to see the fields that you will need to enter yourself."
    print "Example:  createbcHeader.py <sim_name>" 
    sys.exit()

if len(sys.argv) != 2:
    print_usage()

sim_name = sys.argv[1]

#Grabbing excel files from argument vector
excel_files = glob.glob('*.xlsx') 

#Checking to see if they want to average the files
if sys.argv[-1] == '0':
    average_files = False
else:
    average_files = True

#Find out the number of different depths and make dictionary of files and properties
count_t = {}
depth_t = {}

#Make a dictionary of the number of files for each depth
for f in excel_files:
    depth = f.split('.')[0].split('-')[2]
    if not count_t.has_key(depth):
        count_t[depth] = 1
    else:
        count_t[depth] += 1

#Make a dictionary for the disturbed and undisturbed properties at depths 
for key in count_t.iterkeys():
    dist_t = {}
    undist_t = {}
    dist_t['dist_0_'+key] = np.zeros(11) 
    undist_t['undist_0_'+key] = np.zeros(11)
    depth_t[key] = dist_t
    depth_t[key].update(undist_t)


#Make dictionaries for however many more files there are for each depth 
for key in depth_t.iterkeys():
    dist_t = {}
    undist_t = {}
    dist_ave_t = {}
    undist_ave_t = {}
    for k,v in count_t.iteritems():
        if k == key:
            for i in range(v-1):
                dist_t['dist_'+str(i+1)+"_"+k] = np.zeros(11)
                undist_t['undist_'+str(i+1)+"_"+k] = np.zeros(11)
                depth_t[key].update(dist_t)
                depth_t[key].update(undist_t)
    dist_ave_t['dist_ave_'+key] = np.zeros(11)
    undist_ave_t['undist_ave_'+key] = np.zeros(11)
    depth_t[key].update(dist_ave_t)
    depth_t[key].update(undist_ave_t)

#Grabbing allt he data from the sheet and saving them into a overly complicated dictionary
count = 0 
for f in excel_files:
    try:
       book = xlrd.open_workbook(f)
    except:
        print "Unable to open file: " + f
        print "Make sure all excel files are in this directory."
    sheet = book.sheet_by_name('Summary')
    depth = f.split('.')[0].split('-')[2]
    props = np.zeros(11)
    property_list = []
    for i in range(11):
        row = sheet.row_values(22+i)
        dist = row[2:3]
        dist = float(dist[0])
        prop = row[1:2]
        prop = prop[0]
        prop = prop.encode('ascii','ignore')
        property_list.append(prop)
        undist = row[5:]
        undist = float(undist[0])
#        print "depth_t["+depth+"][dist_" + str(count) +"_"+depth+"]["+str(i)+"] = ", dist
#        print "depth_t["+depth+"][undist_" + str(count) +"_"+depth+"]["+str(i)+"] = ", undist

        if i == 4: # This is because residual saturation is a percent in the excel sheets
            depth_t[depth]['dist_'+str(count)+"_"+depth][i] = dist/100.
            depth_t[depth]['undist_'+str(count)+"_"+depth][i] = undist/100.
        else:
            depth_t[depth]['dist_'+str(count)+"_"+depth][i] = dist
            depth_t[depth]['undist_'+str(count)+"_"+depth][i] = undist
    if count_t[depth]-1 == count:
        count = 0
    else:
        count += 1

#Averaging the disturbed and undisturbed properties for each depth
count = 0 
for key, value in depth_t.iteritems():
  for i in range(count_t[key]):
    for k, v in depth_t[key].iteritems(): 
        if k == "dist_"+str(count)+"_"+key:
            for i in range(11):
                depth_t[key]['dist_ave_'+key][i] += depth_t[key][k][i]
        elif k == "undist_"+str(count)+"_"+key:
            for i in range(11):
                depth_t[key]['undist_ave_'+key][i] += depth_t[key][k][i]
    if count_t[depth]-1 == count: 
        count = 0 
    else:
        count += 1

#Dividing each sum by the number of files that went into it to get average
for key in depth_t.iterkeys(): 
    depth_t[key]['dist_ave_'+key] /= count_t[depth]
    depth_t[key]['undist_ave_'+key] /= count_t[depth]

#Creating bc file 
outfile = open(sim_name+'.bc', 'w')
header = '''OP HT
OP GW
OP MET
!OP RAY
!OP SOC 200 300 
OP TRN 0
TC JUL 67  
IP NIT 86  ! Maximum number of non-linear iterations
IP NTL 1e-007 ! Non-Linear absolute tolerance
IP ITL 1e-002  ! Non-Linear incremental tolerance
IP MIT 500  ! Maximum number of linear iterations
OP BLK 8  ! Number of blocks per processor for pre-conditioner
OP PRE 20  ! Preconditioner type
OP INC 8000  ! Incremental memory size
TC T0 0 0   ! Starting time of the simulation
TC TF 720 0  ! Final time of the simulation
TC IDT 1 ! The XY Series that will control the time step size
OC INT 1.0 0
DB FLW 1 3 ! Dirichlet boundary condition for flow PH
NB FLW 2 2 ! Neumann boundary condition for flow GWS
DB TMP 1 5 ! Dirichlet boundary condition for temperature Temp
NB HFX 2 2 ! Heat Flux boundary condition for temperature HTS'''
outfile.write(header+"\n")

#Adding the material types to the bc file
mat_num = 1 

rest_of_prop_ids = ['SS', 'ALB', 'EMS', 'TKA', 'VGP', 'VGX', 'TOR', 'DPL', 'DPT', 'ML', 'FRT']
rest_of_prop_values = ['0.00001','???','???','1 1 1 0 0 0','100','400','0.700','1.0','0.1','0.0','1.0']

for key, value in depth_t.iteritems(): 
    for k, v in depth_t[key].iteritems():
        line = "! material --- " + sim_name + " " + k
        outfile.write(line+'\n')
        for i in range(len(property_list)):
            if i == 1:  # This is because K has three 0's in the bc file
                line = "MP " + property_list[i] + " " + str(mat_num) + " " + str(depth_t[key][k][i]) + " 0 0 0\n"
            else:
                line = "MP " + property_list[i] + " " + str(mat_num) + " " + str(depth_t[key][k][i]) + "\n"
            outfile.write(line)
        for i in range(len(rest_of_prop_ids)): 
            line = "MP " + rest_of_prop_ids[i] + " " + str(mat_num) + " " + rest_of_prop_values[i] + "\n"
            outfile.write(line)
        mat_num += 1 

outfile.write("MP MID bc 3dm\n")
outfile.write("MP MID ???\n")
footer = '''MP G   1.27202e+08  ! Gravity, (m)/(hr^2)
MP SHW .001172  ! Specific heat of water, Units = (W-hr)/(g K)
MP SHG .000347  !  Specific heat of gas, Units = (W-hr)/(g K)
MP SGW 1  ! Specific gravity of water
MP SGG 0.001  ! Specific gravity of gas
MP TKW 0.58  ! Thermal conductivity of water, Units = (W)/(m K)
MP TKG 0.024  ! Thermal conductivity of gas, Units = (W)/(m K)
MP RHO 1e+06  ! Reference density, g/m^3
MP VIS 1e-05  ! Reference viscosity, Units = ?
! function -- Time step size
XY1 1 2 0 0 0 0 
0 0.5
1e+06 0.5

! function -- Top Surface Flux
XY1 2 2 0 0 0 0 
0 -999999 
1e+20 -999999

! function -- Pressure Head
XY1 3 2 0 0 0 0
0 ???
1e+20 ???

! function -- Output Time
XY1 4 1 0 0 0 0
0 0

! function -- Temperature
XY1 5 2 0 0 0 0
0 ???
1e+20 ???



END'''

outfile.write(footer+"\n")
outfile.close()
