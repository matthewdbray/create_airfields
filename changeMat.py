#!/usr/bin/env python 

def read3dm(mesh_name):
    ''' Reads the 3dm file and spits out a list of nodes and facets in that
    order - it takes just the simulation name as an argument '''
    import os 
    cwd = os.getcwd()
    os.chdir(cwd)

    with open(mesh_name) as f1:
        nodes = []
        facets = []
        for line in f1.readlines():
            if line.split()[0] == "E4T":
                facets.append(line)
            if line.split()[0] == "ND":
                nodes.append(line)
        return nodes, facets 

def write3dm(mesh_name, nodes, facets):
    ''' Writes a 3dm file given the new facets and a given mesh_name ''' 
    import os
    cwd = os.getcwd()
    os.chdir(cwd)
    mesh_name = mesh_name.split('.')[0] + '_layers.3dm' 
    with open(mesh_name, 'w') as f1:
        f1.write('MESH3D\n')
        for facet in facets:
            f1.write(facet)
        for node in nodes:
            f1.write(node)

def changeMaterials(nodes, facets, depth, matID, res = 0.01, width = 0.02):
    ''' This changes the material of the 3dm given the depth(s) given and the
    respective matID(s) given as well.  These need to be given as tuples or 
    lists.  The width here is without the ratio.  So if your xy ratio is 
    more than 1:1 use width as if it were 1:1 so it will give the proper
    amount of nodes '''
    def print_usage():
        print '''You need to feed this  - nodes, facets, depth, matID, and 
                optionally the resolution which defaults to 0.01.  Make sure
                that depth is a tuple inside a list and matID is a 
                tuple/list if more than one matID is being used.\n'''
    try:
        if len(depth) != len(matID): 
            print_usage()
    except:
        try:
            assert type(depth) == float
            assert type(matID) == int
        except AssertionError:
            print "Wrong data type for depth or matID"
            sys.exit()

#=============================================================================
#     Reorganizing depths and material IDs 
#=============================================================================
    matID.reverse()
    last_depth = 0.0
    last_depth_adj = 0
    offset = 0 
    dep = []
    print "Mesh properties:"
    for i,d in enumerate(depth):
        print "\tFrom", last_depth, "to", offset+d, "is material ID", matID[i]
        dep.append((last_depth_adj, round(offset+d,5)))
        offset += d
        last_depth+=d
        last_depth_adj=round(last_depth+res,5)
    depth = dep
    print "\tFrom", offset, "to bottom is material ID 1"
#=============================================================================
#    Creating a dictionary for the depths with contain the node numbers (nnums)
#    and the material ID (matID)
#=============================================================================
    nds_per_layer = int(((width/res) + 1)**2)
    depth_dict = {}
    for i,d in enumerate(depth):
        layers = int(round((d[1]-d[0])/res,4))
        layers *= nds_per_layer
        b_nnum = int(d[0]/res)*nds_per_layer
        nnums = (b_nnum+1,int(round(layers+b_nnum,5))) 
        if len(depth) > 1:
            depth_dict[d] = [nnums,matID[i]]
        else:
            depth_dict[d] = [nnums,matID]
    
#=============================================================================
# This looks through the nodes in the facets and changes the material ID if it
# falls within the range of nodes to be changed.  It returns the new facet list.    
#=============================================================================    
    nfacets = []
    for facet in facets:
        newFacet = False
        nds = facet.split()[2:-1]
        nds = [int(x.rstrip('\n')) for x in nds]
        for k,v in depth_dict.iteritems():
            for nd in nds:
                if depth_dict[k][0][0] <= nd <= depth_dict[k][0][-1]:
                    nfacets.append(' '.join(facet.split()[:-1]) + ' ' +\
                            str(depth_dict[k][1])+'\n')
                    newFacet = True
                    break
            if newFacet:
                break
        if not newFacet:
            nfacets.append(facet)
    return nfacets


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--depth', '-d', action='store', type=float, nargs='+',
            default=[0.15, 0.30], dest='depth', 
            help='Depths of the different layers')
    parser.add_argument('--matID', '-m', action='store', type=int, nargs='+',
            dest='matID', default=[2, 3],
            help='Material IDs of different layers')
    parser.add_argument('--res', '-r', action='store', type=float, nargs=1,
            default=0.01, dest='res',
            help='Resolution in Z of the mesh you are changing')
    parser.add_argument('--width','-w', action='store', type=float, nargs=1,
            default=0.02, dest='width', 
            help="The width of x and y assuming xy ratio is 1:1.")
    parser.add_argument('--mesh-name','-n', action='store', nargs=1,
            default='temp.3dm', dest='mesh_name', 
            help='Name of the 3dm mesh you are changing')
    results = parser.parse_args()

#=============================================================================
#     Parsing arguments to variables to pass more easily to the function
#=============================================================================
    depth = results.depth
    matID = results.matID
    res = results.res
    width = results.width
    if type(results.mesh_name) == list:
        mesh_name = results.mesh_name[0]
    else:
        mesh_name = results.mesh_name
#=============================================================================
#     Reading the mesh, changing the materials, and then writing out 
#     the new mesh. 
#=============================================================================
    nodes, facets = read3dm(mesh_name)
    nfacets = changeMaterials(nodes, facets, depth, matID, res, width)
    write3dm(mesh_name, nodes, nfacets) 

