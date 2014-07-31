#!/usr/bin/env python

__author__ = 'matthewbray'

import h5py
import glob
import sys
import os
import numpy
from mpi4py import MPI
### This takes the simulation name as the first argument and the day to extract as the second
### All h5 files must be in the same folder, with no other h5 files (including surface)

sim_name = sys.argv[1]
day_to_extract = sys.argv[2]
total_nodes = int(sys.argv[3])
total_elements = sys.argv[4]

#MPI crap
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

files = glob.glob(sim_name+"_p[0-9]*.h5")
if len(files)%size is 0:
   num_files_per_proc = len(files)/size
   files_per_proc = []
   for i in range(size):
      files_per_proc.append(int(num_files_per_proc))
   files_per_proc = tuple(files_per_proc)

else:
   remainder = len(files)/size
   each_proc = (len(files)-remainder)/size
   files_per_proc = []
   for i in range(size):
      files_per_proc.append(int(each_proc))
   for i in range(remainder):
      files_per_proc[i]+=1
   files_per_proc = tuple(files_per_proc)

if rank is 0:
    print 'Files are divided up in this manner: ', files_per_proc

min_range = rank * files_per_proc[rank]
max_range = rank * files_per_proc[rank] + files_per_proc[rank]

local_files = files[min_range:max_range]

def local_hot_list(filename, day_to_extract, rank):
    f1 = h5py.File(filename, 'r')
    data = f1['Data']
    temp = data['Temperature']
    temp_day = numpy.array(temp[day_to_extract])
    press_head = data['Pressure_Head']
    press_day = numpy.array(press_head[day_to_extract])
    geo = f1['Geometry']
    nodes = geo['orig_node_number']
    orig_nodes = numpy.array(nodes['0'])
    proc_hot_list = numpy.zeros(total_nodes*3)
    proc_hot_list = proc_hot_list.reshape(total_nodes,3)
    #it = numpy.nditer(orig_nodes, flags=['f_index'])
    #while not it.finished:
    #    proc_hot_list[it[0]] = [press_day[it.index],temp_day[it.index], 1]
    #    it.iternext()
    for i in range(len(orig_nodes)):
       proc_hot_list[orig_nodes[i]] = [press_day[i], temp_day[i], 1]
    print 'File ' + filename + ' read by proc', rank
    return proc_hot_list

def add_arrays(args, node_length):
   result = numpy.zeros(node_length*3)
   result = result.reshape(node_length, 3)
   for i in range(len(args)):
      result += args[i]
   return result

a = tuple(local_hot_list(name, day_to_extract, rank) for name in local_files)

complete_hot_list = add_arrays(a, total_nodes)

hot_list = numpy.zeros(total_nodes*3)
hot_list = hot_list.reshape(total_nodes, 3)

comm.Reduce(complete_hot_list, hot_list, op=MPI.SUM, root = 0)

if rank is 0:
    print '--Array Reduced--'

    hotlist = numpy.zeros(total_nodes*2)
    hotlist = hotlist.reshape(total_nodes, 2)
        
    print len(hot_list) 
    print hot_list[-1]
 
    print 'Computing Array....'
    for i in range(len(hot_list)):
        hotlist[i] = [hot_list[i][0]/hot_list[i][2], hot_list[i][1]/hot_list[i][2]]
    print '--Array computed--'
    
    print len(hotlist)
    print hotlist[-1]
## Create hotstart file

    path = os.getcwd()
    path += '/'
    hotstart_file = path + sim_name + '_' + day_to_extract + '.hot'

    f2 = open(hotstart_file, 'w')

    f2.write('DATASET\n')
    f2.write('OBJTYPE "mesh3d"\n')
    f2.write('BEGSCL\n')
    node_line = 'ND ' + str(total_nodes) + '\n'
    f2.write(node_line)
    facet_line = 'NC ' + total_elements + '\n'
    f2.write(facet_line)
    f2.write('NAME "IH"\n')
    f2.write('TS 0 0\n')

    print "Printing pressure heads...."
    for line in hotlist:
       the_press = str(line[0])
       f2.write(the_press)
       f2.write('\n')
    f2.write('ENDDS\n')

    f2.write('DATASET\n')
    f2.write('OBJTYPE "mesh3d"\n')
    f2.write('BEGSCL\n')
    node_line = 'ND ' + str(total_nodes) + '\n'
    f2.write(node_line)
    facet_line = 'NC ' + total_elements + '\n'
    f2.write(facet_line)
    f2.write('NAME "IT"\n')
    f2.write('TS 0 0\n')

    print "Printing temperatures...."
    for line in hotlist:
       the_temp = str(line[1])
       f2.write(the_temp)
       f2.write('\n')
    f2.write('ENDDS\n')
    print "--DONE--"
