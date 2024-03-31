#include "mapreduce.h"

#include <queue>
#include <iostream>
#include <fstream>
#include <numeric>
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;
const unsigned maxmapn = 100;
const unsigned maxredn = 100;
const unsigned minfsize = 1000;

mapper::mapper(std::string fname, long offset, long blocksize)
  : m_fname(std::move(fname)), m_offset(offset), m_blocksize(blocksize)
{
  std::cout << "mapper offset: " << offset << " blocksize: " << blocksize << "\n";
}

void mapper::domap()
{
  std::ifstream ifs(m_fname, std::ios_base::in|std::ios::binary);
  if(!ifs.is_open()) return;
  ifs.seekg(m_offset);
  for(std::string line; ifs.tellg() < (m_offset + m_blocksize) && std::getline(ifs, line);)
  {
    m_data.emplace_back(line);
  }
  std::sort(m_data.begin(), m_data.end());
}

void mapper::start_parallel()
{
  m_th = std::thread(&mapper::domap, this);
}

void mapper::join()
{
  m_th.join();
}

size_t mapper::osize() const
{
  return m_data.size();
}

// template <typename in_it_t, typename out_it_t>
// void shuffle(std::vector<in_it_t> &inbegin, std::vector<in_it_t> &inend, std::vector<out_it_t> &outbegin)
// {

// }

void shuffle(
  size_t insize,
  std::vector<mapper::out_const_iterator_t> &inbegin,
  std::vector<mapper::out_const_iterator_t> &inend,
  std::vector<reducer::in_inserter_t> &outbegin)
{
  auto incur = inbegin;
  const auto mapn = inbegin.size();
  const auto redn = outbegin.size();
  std::priority_queue<std::pair<mapper::out_elem_t, unsigned>> heads;
  for(unsigned ii = 0; ii < mapn; ++ii) {
    if(inbegin[ii] != inend[ii]) {
      heads.emplace(*(inbegin[ii]),ii);
    }
  }

    // split output
  const auto outblocksize = insize / redn;
  auto outcur = outbegin;
  unsigned curoutblock = 0;
  unsigned long curoutsize = 0;
  while(!heads.empty()) {
    auto const &topval = heads.top();
    *(outcur[curoutblock]) = topval.first;
    curoutsize++;
    if(curoutsize >= outblocksize && curoutblock < redn) {
      curoutblock++;
    } else {
      outcur[curoutblock]++;
    }
    incur[topval.second]++;
    heads.pop();
    if(incur[topval.second] != inend[topval.second])
    {
      heads.emplace(*(incur[topval.second]), topval.second);
    }
  }
}


mapreduce::mapreduce(std::string fname, unsigned mapn, unsigned redn)
    : m_fname(std::move(fname)), m_mapn{mapn}, m_redn{redn}
{
  if(m_mapn < 1) throw std::runtime_error("map number must be >=1");
  if(m_redn < 1) throw std::runtime_error("reduce number must be >=1");
  if(m_mapn > maxmapn) throw std::runtime_error("map number must be <=100");
  if(m_redn > maxredn) throw std::runtime_error("reduce number must be <=100");
  if(!fs::is_regular_file(m_fname)) throw std::runtime_error("input is not a regular file");
  const long fsize = (long)boost::filesystem::file_size(m_fname);
  std::cout << m_fname << " size is "  << fsize << "B\n";
  if(fsize < minfsize)  throw std::runtime_error("file is too small");

  const long blocksize = fsize / mapn;
  std::cout << "blocksize is "  << blocksize << "B\n";
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
    m_maps.emplace_back(m_fname, offset, newoffset - offset);
    offset = newoffset;
  }
  for(auto && mapper : m_maps) {
    mapper.start_parallel();
  }
  for(auto && mapper : m_maps) {
    mapper.join();
  }
    // shuffle
  std::vector<mapper::out_const_iterator_t> mobegin;
  std::vector<mapper::out_const_iterator_t> moend;
  for(auto && mapper : m_maps) {
    mobegin.emplace_back(mapper.outbegin());
    moend.emplace_back(mapper.outend());
  }

  std::vector<reducer::in_inserter_t> ribegin;
  for(unsigned ii = 0; ii < redn; ii++) {
    auto &red = m_reds.emplace_back();
    ribegin.emplace_back(red.backinserter());
  }
  shuffle(size(), mobegin, moend, ribegin);
}

size_t mapreduce::size() const
{
  unsigned sumsize{0};
  for(auto && mapper: m_maps)
  {
    sumsize += mapper.osize();
  }
  return sumsize;
}
