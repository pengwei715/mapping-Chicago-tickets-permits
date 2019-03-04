'''
CAPP 30122: Test code for Data Compression assignment

Lamont Samuels
Febrauary 2019
'''

# DO NOT REMOVE THESE LINES OF CODE
# pylint: skip-file

import os
import sys
import pytest

from compressors import RLECompressor,WordCompressor

# Get the name of the directory that holds the grading code.
#BASE_DIR = os.path.dirname(__file__)
#TEST_DATA_DIR = os.path.join(BASE_DIR, "speeches")

MAX_TIME = 120
EPSILON = 0.00001

COMPRESSION_DATA = [
      ("CCC", "CC", [3]),
      ("CCCcccCCC", "CCccCC", [3,3,3]),
      ("CCCAaAaAacccccCCC", "CCAaAaAaccCC", [3,5,3]),
      ("Abcdefg", "Abcdefg", []),
      ("111aa345nmmmmmnmm33", "11aa345nmmnmm33", [3,2,5,2,2]),
      ("     ___\t98\t\t\t75\n\n\n\n", "  __\t98\t\t75\n\n", [5,3,3,4]),
      ("WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWWWWWWWWWWWWWWWWWWBWWWWWWWWWWWWWW", "WWBWWBBWWBWW", [12,12,3,24,14]),
    ]

LYRICS_COMPRESSION_DATA = [
      ("No\trepeated\twords\t\t",
       "No\trepeated\twords\t\t",
       {}
      ),
      ( ("Time after time\n"
         "If you fall I will catch you, I will be waiting\n"
         "Time after time\n"
        ),
        (":\x00 :\x01 :\x02\n"
         "If you fall :\x03 :\x04 catch you, :\x03 :\x04 be waiting\n"
         ":\x00 :\x01 :\x02\n"
        ),
        {":\x00":"Time",
         ":\x01":"after",
         ":\x02":"time",
         ":\x03":"I",
         ":\x04":"will"}
      ),
      ("Oh, oh, oh, oh, oh, oh",
       "Oh, :\x00 :\x00 :\x00 :\x00 oh",
       {":\x00":"oh,"}
      ),
      ( ("All the single ladies\n"
         "(All the single ladies)\n"
         "All the single ladies\n"
         "(All the single ladies)\n"
         "All the single ladies\n"
         "(All the single ladies)\n"
         "All the single ladies\n"
         "Now put your hands up\n"),
        (":\x00 :\x01 :\x02 :\x03\n"
         ":\x04 :\x01 :\x02 :\x05\n"
         ":\x00 :\x01 :\x02 :\x03\n"
         ":\x04 :\x01 :\x02 :\x05\n"
         ":\x00 :\x01 :\x02 :\x03\n"
         ":\x04 :\x01 :\x02 :\x05\n"
         ":\x00 :\x01 :\x02 :\x03\n"
         "Now put your hands up\n"),
       {":\x00":"All",
        ":\x01":"the",
        ":\x02":"single",
        ":\x03":"ladies",
        ":\x04":"(All",
        ":\x05":"ladies)"}
      ),

    ]
################## RLE Compression Tests ##################
def run_rle_decompression_test(expected_uncompressed,
                               expected_compressed,
                               expected_run_counts):
    '''
    Run a specific test.
    '''
    # Create a compressor
    compressor = RLECompressor()

    # Retrieve the compressed values
    compressor.read(expected_compressed)
    compressor.run_lengths = expected_run_counts
    actual = compressor.decompress()

    #Check to make sure a tuple was returned by compress
    if not isinstance(actual, str):
        s = "Actual value returned from decompress must be a string"
        pytest.fail(s)

    # Check to to see if the actual decompressed encoding
    # matches the expected decompressed encoding
    if  actual != expected_uncompressed:
        s = ("Decompressing:({})\nActual decompressed string ({}) and expected decompressed string"
             "({}) values do not match")
        pytest.fail(s.format(expected_compressed,actual,expected_uncompressed))

def run_rle_compression_test(expected_uncompressed,
                             expected_compressed,
                             expected_run_counts):
    '''
    Run a specific test.
    '''
    # Create a compressor
    compressor = RLECompressor()

    # Read in data to compress
    compressor.read(expected_uncompressed)

    # Retrieve the compressed values
    actual = compressor.compress()

    #Check to make sure a tuple was returned by compress
    if not isinstance(actual, str):
        s = "Actual value returned from compress must be a str"
        pytest.fail(s)

    # Check to to see if the actual compressed encoding
    # matches the expected compressed encoding
    if  actual != expected_compressed:
        s = ("actual compressed encoding ({}) and expected compressed encoding "
             "({}) values do not match")
        pytest.fail(s.format(actual,expected_compressed))

    # Check to to see if the actual run_lengths are equal
    # matches the expected run_lengths
    if  compressor.run_lengths != expected_run_counts:
        s = ("actual run_lengths ({}) and expected run_lengths"
             "({}) values do not match")
        pytest.fail(s.format(compressor.run_lengths,expected_run_counts))

################## Word Compression Tests ##################
def run_word_compression_test(expected_uncompressed,
                              expected_compressed,
                              expected_dictionary):

    # Create a compressor
    compressor = WordCompressor(":")

    # Read in data to compress
    compressor.read(expected_uncompressed)

    # Retrieve the compressed values
    actual = compressor.compress()

    #Check to make sure a tuple was returned by compress
    if not isinstance(actual, str):
        s = "Actual value returned from compress must be a str"
        pytest.fail(s)

    # Check to to see if the actual compressed encoding
    # matches the expected compressed encoding
    if actual != expected_compressed:
        s = ("actual compressed encoding ({}) and expected compressed encoding "
             "({}) values do not match")
        pytest.fail(s.format(actual,expected_compressed))

    # Check to to see if the actual run_lengths are equal
    # matches the expected run_lengths
    if  compressor.encoded_dict != expected_dictionary:
        s = ("actual encoded_dict ({}) and expected encoded_dict"
             "({}) values do not match")
        pytest.fail(s.format(compressor.encoded_dict,expected_dictionary))

def run_word_decompression_test(expected_uncompressed,
                              expected_compressed,
                              expected_dictionary):

    # Create a compressor
    compressor = WordCompressor(":")

    # Read in data to compressor
    compressor.encoded_dict = expected_dictionary
    compressor.read(expected_compressed)

    # Retrieve the compressed values
    actual = compressor.decompress()

    #Check to make sure a tuple was returned by compress
    if not isinstance(actual, str):
        s = "Actual value returned from compress must be a str"
        pytest.fail(s)

    # Check to to see if the actual compressed encoding
    # matches the expected compressed encoding
    if  actual != expected_uncompressed:
        s = ("Decompressing:({}) actual deccompressed encoding ({}) and expected decompressed encoding "
             "({}) values do not match")
        pytest.fail(s.format(expected_compressed,actual,expected_uncompressed))

@pytest.mark.parametrize("uncompressed,compressed, run_counts", COMPRESSION_DATA)
def test_rlecompress(uncompressed, compressed,run_counts):
    '''
        Test harness. This test checks that compress works correctly.
    '''
    run_rle_compression_test(uncompressed, compressed, run_counts)

@pytest.mark.parametrize("uncompressed, compressed, run_counts", COMPRESSION_DATA)
def test_rledecompress(uncompressed, compressed, run_counts):
    '''
        Test harness. This test checks if decompress works correctly.
    '''
    run_rle_decompression_test(uncompressed, compressed, run_counts)

@pytest.mark.parametrize("uncompressed,compressed, encoded_dict", LYRICS_COMPRESSION_DATA)
def test_wordcompress(uncompressed, compressed,encoded_dict):
    '''
        Test harness. This test checks that compress works correctly.
    '''
    run_word_compression_test(uncompressed, compressed, encoded_dict)

@pytest.mark.parametrize("uncompressed, compressed, encoded_dict", LYRICS_COMPRESSION_DATA)
def test_worddecompress(uncompressed, compressed, encoded_dict):
    '''
        Test harness. This test checks if decompress works correctly.
    '''
    run_word_decompression_test(uncompressed, compressed, encoded_dict)
