#pragma once

#include "mapreduce.h"
#include <string>
#include <vector>

/** @brief Load map from single text file */
class MapFileLoader
{
public:
    /**
     * @param fname source file name
     * @param mapn amount of map threads
     * */
  MapFileLoader(std::string fname, unsigned mapn);

    /**
    *  That job will be executed in parallel
    *  @param blocknumber block number
    *  @param iit inserter iterator for the mapper container
    *  */
  void load(int blocknumber, mapper::inserter_t iit);

private:
  std::string m_fname;
  std::vector<long> m_offset;
  std::vector<long> m_blocksize;
};
