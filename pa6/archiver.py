'''
CAPP30122 W'19: Data Compression

Your name
'''

import sys
from compressors import RLECompressor,WordCompressor,Compressor

class ArchivedFile:
    '''
    Class for representing an archived file
    '''
    def __init__(self,name,files,compressor=Compressor()):
        '''
        Constructs an archived file with the files that
        are part of the archive along with the compressor
        that compresss the files.

        Inputs
            name  (string): the name of the archived file
            files ([string]): the list of files that are part
                              of the archived

            compressor (Compressor): the compressor use to
                              compress the files in the archiver

        '''
        self.name = name
        self.files = files
        self.compressor = compressor


def find_best_compressor(archived_files, compressors):
    '''
    Updates each compressor attribute inside the list of archived files to
    the compressor with the greatest compression ratio inside the list of
    compressors. The function does not update the compressor attribute
    if all the compressors inside the list of compressors produces
    a compression ratio greater than the compressor already inside an
    archived file.


    Inputs:
        archived_files ([ArchivedFile]) : a list of archived files
        compressors ([Compressors]): a list of compressors
    '''
    #YOUR CODE HERE
