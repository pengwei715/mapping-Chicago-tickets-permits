'''
CAPP30122 W'19: Data Compression

Your name
'''

import sys
import re

class Compressor:
    '''
    Class for representing a way to compress and decompress text data.
    '''
    def __init__(self):
        self._buffer = ""

    def read(self, input_data):
        '''
        Reads in the input_data into the buffer of the decompressor.

        Input:
        input_data (string): the data into insert into the buffer

        '''
        self._buffer = self._buffer + input_data

    def clear(self):
        '''
        Clears the contents of the internal buffer
        '''
        self._buffer = ""

    def compress(self):
        '''
        Compresses the data inside the internal buffer.

        Output:
            A string of the compressed data
        '''
        # YOUR CODE HERE
        pass

    def decompress(self):
        '''
        Restores the uncompressed string (i.e, original data) from the
        encoded text data buffer.

        Output:
            The original data of the text buffer as a string
        '''
        # YOUR CODE HERE
        pass

    @property
    def ratio(self):
        '''
        Computes the compression ratio

            ratio = uncompressed size / compressed size

        Output:
            a number of representing the compression ratio
        '''
        # YOUR CODE HERE
        pass

class RLECompressor:

    MIN_RUN_LENGTH = 2

    '''
    Class for representing a way to compress and decompress text data
    using the run length algorithm
    '''
    def __init__(self):
        '''
        Constructs a run-length compressor that initializes the internal buffer
        and clears its run_lengths property.
        '''
        # YOUR CODE HERE
        pass


class WordCompressor:

    def __init__(self, delimiter):
        '''
        Constructor a compressor with the given delimiter and sets the encoded dictionary
        to be initially empty.
        '''
        pass
