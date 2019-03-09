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

from compressors import Compressor,RLECompressor,WordCompressor
from archiver import ArchivedFile,find_best_compressor

# Get the name of the directory that holds the grading code.
#BASE_DIR = os.path.dirname(__file__)
#TEST_DATA_DIR = os.path.join(BASE_DIR, "data")

COMPRESSORS_1 = [Compressor(), RLECompressor(),WordCompressor(":")]
COMPRESSORS_R = [Compressor(), RLECompressor(), Compressor(), WordCompressor(":")]
RLECOMPRESSOR_R = [Compressor(), WordCompressor(":"), RLECompressor(), RLECompressor()]
WORDCOMPRESSOR_R = [WordCompressor(":"),  RLECompressor(), WordCompressor(":"), Compressor()]

TEST_DATA = [
      (COMPRESSORS_1,[2,2,1,None]),
      (COMPRESSORS_R,[3,3,1,None]),
      (RLECOMPRESSOR_R,[1,1,2,None]),
      (WORDCOMPRESSOR_R,[0,0,1,None]),
    ]

TEST_DATA_2 = [
      (COMPRESSORS_1,[2,None,1,None],[RLECompressor(),WordCompressor(":"),WordCompressor(":"),WordCompressor(":")]),
      (WORDCOMPRESSOR_R,[0,None,1,None],[RLECompressor(),WordCompressor(":"),WordCompressor(":"),RLECompressor()]),
      (RLECOMPRESSOR_R,[1,1,2,None],[RLECompressor(),RLECompressor(),WordCompressor(":"),Compressor()])
    ]

################## find_best_compressor Test ##################
def run_archives_test(archives, compressors, original_compressors, expected):

    def str_compressor(compressor):
      if isinstance(compressor,WordCompressor):
          return "WordCompressor(delimiter=" + compressor.delimiter + ")"
      elif isinstance(compressor,RLECompressor):
          return "RLECompressor()"
      elif isinstance(compressor,Compressor):
          return "Compressor()"
      else:
          return "Invalid Compressor"

    def str_archive(archive):
        return "ArchiveFile(Name =" + archive.name + " files=" + str(archive.files) + " " +  "(" + str_compressor(archive.compressor) + ", id =" + str(id(archive.compressor)) + "))"

    # Call find_best_compressor
    find_best_compressor(archives,compressors)

    for idx,file in enumerate(archives):
        compressor_idx = expected[idx]
        if compressor_idx is None:
            compressor = original_compressors[idx]
        else:
            compressor = compressors[compressor_idx]
        #Check if the compressors match expected
        if id(compressor) != id(file.compressor):
            s = "Archive File ({})\n actual compressor ({}, id = {}) does not match expected Compressor({}, id ={})\n"
            pytest.fail(s.format(str_archive(file),
                                 str_compressor(file.compressor),
                                 id(file.compressor),
                                 str_compressor(compressor),
                                 id(compressor)))

@pytest.mark.parametrize("compressors,expected", TEST_DATA)
def test_archives_1(compressors,expected):
    '''
        Test harness. This test checks that find_best_compressor works.
    '''
    lyrics_archive = ArchivedFile("lyrics.ar",["data/lyrics1.txt", "data/lyrics2.txt", "data/lyrics3.txt", "data/lyrics4.txt", "data/lyrics5.txt"])
    regular_archive = ArchivedFile("regular_text.ar",["data/regular_text1.txt", "data/regular_text2.txt", "data/regular_text3.txt", "data/regular_text4.txt", "data/regular_text5.txt"])
    rle_archive = ArchivedFile("rle.ar",["data/rle1.txt", "data/rle2.txt", "data/rle3.txt", "data/rle4.txt", "data/rle5.txt"])
    names_archive = ArchivedFile("names.ar",["data/names1.txt", "data/names2.txt", "data/names3.txt", "data/names4.txt"])

    archives = [lyrics_archive, regular_archive, rle_archive, names_archive]

    origs = []
    for file in archives:
        origs.append(file.compressor)
    run_archives_test(archives, compressors, origs, expected)

@pytest.mark.parametrize("compressors,expected,initial", TEST_DATA_2)
def test_archives_2(compressors,expected,initial):
    '''
        Test harness. This test checks that find_best_compressor works.
    '''
    #lyrics (Word), regular (Word), rle (RLE), names (Compressor)
    lyrics_archive = ArchivedFile("lyrics.ar",["data/lyrics1.txt", "data/lyrics2.txt", "data/lyrics3.txt", "data/lyrics4.txt", "data/lyrics5.txt"])
    regular_archive = ArchivedFile("regular_text.ar",["data/regular_text1.txt", "data/regular_text2.txt", "data/regular_text3.txt", "data/regular_text4.txt", "data/regular_text5.txt"])
    rle_archive = ArchivedFile("rle.ar",["data/rle1.txt", "data/rle2.txt", "data/rle3.txt", "data/rle4.txt", "data/rle5.txt"])
    names_archive = ArchivedFile("names.ar",["data/names1.txt", "data/names2.txt", "data/names3.txt", "data/names4.txt"])

    archives = [lyrics_archive, regular_archive, rle_archive, names_archive]

    for idx, file in enumerate(archives):
        file.compressor = initial[idx]

    origs = []
    for file in archives:
        origs.append(file.compressor)
    run_archives_test(archives, compressors, origs, expected)
