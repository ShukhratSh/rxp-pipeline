# Derived from https://github.com/ShukhratSh/rxp-pipeline/blob/main/tile_index.py

#!/usr/bin/env python
from datetime import datetime
start = datetime.now()

import pandas as pd
import os, glob
import multiprocessing
import argparse
import json
import pdal

def las2ply(las, args):
    
    if args.verbose:
        with args.Lock:
            print('converting:', las)

    reader = {"type":"readers.las",
              "filename":las}
    
    writer = {'type':'writers.ply',
             # 'storage_mode':'little endian',
              'filename':os.path.join(args.odir, os.path.split(las)[1].replace('.las', '.ply'))}
            
    cmd = json.dumps([reader, writer])
    pipeline = pdal.Pipeline(cmd)
    pipeline.execute()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--idir', type=str, help='directory where downsampled tiles are stored')
    parser.add_argument('-o','--odir', default='.', help='directory where downsampled tiles are stored')
    #parser.add_argument('-l', '--length', type=float, default=.02, help='voxel edge length')
    parser.add_argument('--num-prcs', type=int, default=10, help='number of cores to use')
    parser.add_argument('--verbose', action='store_true', help='print something')
    args = parser.parse_args()
    
    m = multiprocessing.Manager()
    args.Lock = m.Lock()
    pool = multiprocessing.Pool(args.num_prcs)
    pool.starmap_async(las2ply, [(las, args) for las in glob.glob(os.path.join(args.idir, '*.las'))])
    pool.close()
    pool.join() 

    if args.verbose: print(f'runtime: {(datetime.now() - start).seconds}')
