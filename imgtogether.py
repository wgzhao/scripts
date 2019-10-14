#!/usr/bin/env python3
"""\
    Utility which put some movie screenshots together, each screenshot only reserved subtitle

    $0 -d ./screenshots -r sc_*.png -s 0.6  -p bottom -o all.png 
"""
import sys
import os
import cv2
import numpy as np
from glob import glob
from argparse import ArgumentParser

def together(args):
    if args.basedir.startswith('~'):
        folder = os.path.expanduser(args.basedir)
    else:
        folder = args.basedir
    files = glob(f"{folder}/{args.wild}")
    img = cv2.imread(files[0])
    height = img.shape[0]
    for f in files[1:]:
        curr_img = cv2.imread(f)
        if args.subpos == 'bottom':
            img = np.concatenate([img, curr_img[int(height*0.85):,:,:], ], axis=0)
        else:
            img = np.concatenate([img, curr_img[:int(height*0.15),:,:], ], axis=0)

    #resize image
    cv2.imwrite(f'{args.output}', 
            cv2.resize(img, 
                    dsize=(int(img.shape[1]*args.resize),
                        int(img.shape[0]*args.resize))
                    )
                )
    return 0

if __name__ == '__main__':
    
    parser = ArgumentParser(description='put movie screenshots together')
    parser.add_argument('-d','--dir', dest='basedir', help='screenshot directory', default='.')
    parser.add_argument('-o','--output', dest='output', 
                help='output picture name out.png by default', default='out.png')
    parser.add_argument('-p','--subtitle',dest='subpos', 
            help='the position of subtitle,you can choose bottom and top', 
            choices=['bottom','top'], default='bottom')
    parser.add_argument('-r','--regex', dest='wild', help='the pattern match pictures, all files in current directory by default', default='*.*')
    parser.add_argument('-s','--resize', dest='resize', type=float, help='resize the output picture, specify 0.8 if you wan reduce to 80 percent of original picture, 1 by default', default=1)
    args = parser.parse_args()

    if args.resize > 1:
        print("the resize must be not larger than 1")
        sys.exit(2)
    sys.exit(together(args))
    
