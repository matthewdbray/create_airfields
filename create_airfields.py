#!/usr/bin/env python

from meshpy.tet import MeshInfo, build, Options
import sys, os

mesh_info = MeshInfo()

def print_usage(): 
    print "usage: this produces by default a 0.2 width and 1.0 meter depth cube\n"
    print "ex: create_airfield.py <sim_name> <thickness of con/asp> <thickness of base>\n"
    print "optionally you can include dx dy dz at the end of the command\n"

if len(sys.argv) < 4 or len(sys.argv) > 7: 
    print_usage()
    sys.exit() 

sim_name = sys.argv[1] + '_'
name_args = sys.argv[2:]
print name_args
sim_name += '_'.join(name_args)
delta_con = float(sys.argv[2])
delta_base = float(sys.argv[3])

# construct a two-box extrusion of this base
base = [(-0.2,-0.2,0), (0.2,-0.2,0), (0.2,0.2,0), (-0.2,0.2,0)]  #TODO make this 0.2 variable

points = base + [(x,y,z+(1-delta_base-delta_con)) for x,y,z in base] #base + first box
points += [(x,y,z+(1-delta_con)) for x,y,z in base] # second box
points += [(x,y,z+1) for x,y,z in base] # third box

pnt_markers=[]

for elem in points:
    if elem in points and elem in base: 
        pnt_markers.append(-1)
    else:
        pnt_markers.append(0)

# first, the nodes
mesh_info.set_points( points,
        pnt_markers
        )


# next, the facets

# vertex indices for a box missing the -z face
box_without_minus_z = [
    [4,5,6,7],
    [0,4,5,1],
    [1,5,6,2],
    [2,6,7,3],
    [3,7,4,0],
    ]

def add_to_all_vertex_indices(facets, increment):
    return [[pt+increment for pt in facet] for facet in facets]

facets =  [[0,1,2,3]] + box_without_minus_z # first box
facets += add_to_all_vertex_indices(box_without_minus_z, 4)# second box
facets += add_to_all_vertex_indices(box_without_minus_z, 8)# third box

facet_mrks = []
for f in facets:
    facet_mrks.append(-(int(f == max(facets))))  # Finding the surface boundary facet and labeling negative

mesh_info.set_facets(facets, 
        facet_mrks   # Surface boundary conditions
    )

# figuring out what each of the volume constraints should be
# the edge length here is divided by four to make sure there are at least 3 nodes per layer

vc = lambda x: (x/4)**3/6


# set the volume properties -- this is where the tet size constraints are
mesh_info.regions.resize(3)
mesh_info.regions[0] = [0,0,1-delta_con/2,# point in volume -> first box
        10, # region tag (user-defined number)
        vc(delta_con), # max tet volume in region
        ]
mesh_info.regions[1] = [0,0,((1-delta_con)-delta_base/2), # point in volume -> second box
        20, # region tag (user-defined number, arbitrary)
        vc(delta_base), # max tet volume in region
        ]

mesh_info.regions[2] = [0,0,(1-delta_con-delta_base)/2, # point in volume -> second box
        30, # region tag (user-defined number, arbitrary)
        vc(1-delta_con-delta_base), # max tet volume in region
        ]

mesh = build(mesh_info, options=Options("pqnn"), volume_constraints=True, attributes=True)


for facet in mesh.facets:
  print facets
# this is a no-op, but it shows how to access the output data
#for point in mesh.points:
#     [x,y,z] = point

#for element in mesh.elements:
#    [pt_1, pt_2, pt_3, pt_4] = element

# this writes the mesh as a vtk file, requires pyvtk
#mesh.write_vtk("test.vtk")
mesh.write_3dm(sim_name+".3dm")
mesh.write_boundary(sim_name+".bc")
