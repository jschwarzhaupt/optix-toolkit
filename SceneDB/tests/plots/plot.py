#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os

def plotObjectSize():
    filename = 'object size.csv'
    df = pd.DataFrame()
    df = pd.read_csv(filename)
    fig, ax = plt.subplots()
    ax.set_title('Read throughput vs. object size\n(cold cache)')
    ax.set_xlabel('Object size (K)')
    ax.set_ylabel('Throughput (MB/s)')
    ax.set_xlim(0, 16*1024)
    ax.plot(df['object size'], df['read throughput'], 'g.-')
    outfile = os.path.splitext(filename)[0] + '.png'
    plt.savefig(outfile)
    plt.show()

def plotThreadingWith4KObjects():
    filename = 'threading with 4K objects.csv'
    df = pd.DataFrame()
    df = pd.read_csv(filename)
    fig, ax = plt.subplots()
    ax.set_title('Read throughput vs. threads\n(warm cache)')
    ax.set_xlabel('threads')
    ax.set_ylabel('Throughput (MB/s)')
    ax.plot(df['num threads'], df['read throughput'], 'g.-')
    ax.set_xlim(1, 18)
    ax.set_ylim(ymin=0)
    outfile = os.path.splitext(filename)[0] + '.png'
    plt.savefig(outfile)
    plt.show()
    
if __name__ == '__main__':
    plotObjectSize()
    plotThreadingWith4KObjects()
