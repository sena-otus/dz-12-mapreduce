#include "mapreduce.h"

#include <queue>
#include <iostream>
#include <fstream>
#include <numeric>
#include <boost/filesystem.hpp>
#include <string_view>

namespace fs = boost::filesystem;
const unsigned maxmapn = 100;
const unsigned maxredn = 100;
const unsigned minfsize = 1000;

mapper::mapper(std::string fname, long offset, long blocksize)
  : m_fname(std::move(fname)), m_offset(offset), m_blocksize(blocksize)
{
  // std::cout << "mapper offset: " << offset << " blocksize: " << blocksize << "\n";
}

void mapper::doit()
{
  std::ifstream ifs(m_fname, std::ios_base::in|std::ios::binary);
  if(!ifs.is_open()) return;
  ifs.seekg(m_offset);
  for(std::string line; ifs.tellg() < (m_offset + m_blocksize) && std::getline(ifs, line);) {
    m_data.emplace_back(line);
  }
  std::sort(m_data.begin(), m_data.end());
}

void mapper::start_parallel()
{
  m_th = std::thread(&mapper::doit, this);
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
  const size_t insize,
  const std::vector<mapper::out_const_iterator_t> &inbegin,
  const std::vector<mapper::out_const_iterator_t> &inend,
  const std::vector<reducer::in_inserter_t> &outbegin)
{
  auto incur = inbegin;
  const auto mapn = inbegin.size();
  const auto redn = outbegin.size();
  std::priority_queue<std::pair<mapper::out_elem_t, unsigned>,
                      std::vector<std::pair<mapper::out_elem_t,unsigned>>,
                      std::greater<>> heads;

  for(unsigned ii = 0; ii < mapn; ++ii) {
    if(incur[ii] != inend[ii]) {
      heads.emplace(*(incur[ii]),ii);
    }
  }

    // caclulate output container size
  const auto outblocksize = insize / redn;
  auto outcur = outbegin;
  unsigned curoutblock = 0;
  unsigned long curoutsize = 0;
  while(!heads.empty()) {
    auto const &topval = heads.top();
    *(outcur[curoutblock]) = topval.first;
    auto initidx = topval.second;
    heads.pop();
    curoutsize++;
    if(curoutsize >= outblocksize && (curoutblock+1) < redn) {
      curoutblock++;
      curoutsize = 0;
    } else {
      outcur[curoutblock]++;
    }
    incur[initidx]++;
    if(incur[initidx] != inend[initidx])
    {
      heads.emplace(*(incur[initidx]), initidx);
    }
  }
}

reducer::reducer(std::string fname)
  : m_fname(std::move(fname))
{
}


void reducer::doit()
{
  std::ofstream ofs(m_fname, std::ios_base::out|std::ios::binary);
  if(!ofs.is_open()) return;
  std::string prevstr;
  unsigned curprefixlen = 0;
  for(auto && curstr : m_in)
  {
    if(curstr.length() < curprefixlen) {
      continue;
    }
    auto prevprefix = std::string_view(prevstr.data(), curprefixlen);
    auto curprefix  = std::string_view(curstr .data(), curprefixlen);
    if(prevprefix != curprefix) {
      prevstr = curstr;
      break;
    }
    auto oldprefixlen = curprefixlen;
    do
    {
      curprefixlen++;
      const bool curstr_tooshort  = curprefixlen > curstr .length();
      const bool prevstr_tooshort = curprefixlen > prevstr.length();
      if(curstr_tooshort && prevstr_tooshort) { // duplicate
        curprefixlen = oldprefixlen;
        break;
      }
      if(prevstr_tooshort) {
        prevstr = curstr;
        break;
      }
      if(curstr_tooshort) {
        break;
      }
    } while(curstr[curprefixlen-1] == prevstr[curprefixlen-1]);
  }
  std::cout << "curprefixlen: " << curprefixlen << "\n";
}

void reducer::start_parallel()
{
  m_th = std::thread(&reducer::doit, this);
}

void reducer::join()
{
  m_th.join();
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
  // std::cout << m_fname << " size is "  << fsize << "B\n";
  if(fsize < minfsize)  throw std::runtime_error("file is too small");

  const long blocksize = fsize / mapn;
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
    auto &red = m_reds.emplace_back("reduce_out");
    ribegin.emplace_back(red.backinserter());
  }
  shuffle(size(), mobegin, moend, ribegin);
  // for(auto && red : m_reds) {
  //   for(auto it = red.in_cbegin(); it != red.in_cend(); ++it) {
  //     std::cout << *it << "\n";
  //   }
  // }

  // reduce
  for(auto && red : m_reds) {
    red.start_parallel();
  }
  for(auto && red : m_reds) {
    red.join();
  }

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
