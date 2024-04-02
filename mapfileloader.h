#pragma once

#include "mapreduce.h"
#include <string>
#include <vector>

class MapFileLoader
{
public:
  MapFileLoader(std::string fname, unsigned mapn);

  void load(int blocknumber, mapper::inserter_t iit);

private:
  std::string m_fname;
  std::vector<long> m_offset;
  std::vector<long> m_blocksize;
};
