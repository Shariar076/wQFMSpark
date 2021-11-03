# import os
# import pandas as pd
# import sys
import subprocess
import argparse
# import re
# import time
# from collections import OrderedDict
from normalize_weights import normalize_quartets_weights

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--tag", required=True, help="tag")
parser.add_argument("-i", "--input", required=True, help="input")

args = parser.parse_args()

cat = subprocess.Popen(["/home/himel/.hadoop/bin/hadoop", "fs", "-cat", f"{args.input}/tag={args.tag}/*.csv"], stdout=subprocess.PIPE)
for line in cat.stdout:
    print(line.decode('utf-8'))