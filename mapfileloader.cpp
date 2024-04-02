#include "mapfileloader.h"

#include <boost/filesystem.hpp>

const unsigned minfsize = 1000;

MapFileLoader::MapFileLoader(std::string fname, unsigned mapn)
  : m_fname(std::move(fname)),
    m_offset(mapn), m_blocksize(mapn)
{
  const long fsize = (long)boost::filesystem::file_size(m_fname);
    // std::cout << m_fname << " size is "  << fsize << "B\n";
  if(fsize < minfsize)  throw std::runtime_error("file is too small");

  auto blocksize = fsize / mapn;
    // std::cout << "blocksize is "  << blocksize << "B\n";

  std::ifstream fin(m_fname, std::ios_base::in|std::ios::binary);
  long offset = 0;
  for(unsigned ii = 0; ii < mapn; ++ii) {
    fin.seekg((ii+1) * blocksize);
      // read until newline
    std::string line;
    std::getline(fin, line);
    long newoffset = fin.tellg();
    if(newoffset == -1) {
      newoffset = fsize-1;
    }
    m_offset[ii] = offset;
    m_blocksize[ii] = newoffset - offset;
    offset = newoffset;
  }
}

void MapFileLoader::load(int blocknumber, mapper::inserter_t iit)
{
  std::ifstream ifs(m_fname, std::ios_base::in|std::ios::binary);
  if(!ifs.is_open()) return;
  ifs.seekg(m_offset[blocknumber]);
  const long stopoffset =  m_offset[blocknumber] + m_blocksize[blocknumber];
  for(std::string line; ifs.tellg() < stopoffset && std::getline(ifs, line);) {
      *iit = line;
  }
}
