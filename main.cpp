/**
 * @file main.cpp
 * @brief Exercise 12, mapreduce
 *  */

#include "mapreduce.h"
#include "mapfileloader.h"
#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <ios>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <regex>
#include <string>
#include <queue>
#include <utility>


const int generic_errorcode = 1;

const unsigned maxmapn = 128;
const unsigned maxredn = 128;


void shuffle(
  const size_t insize,
  const std::vector<mapper::const_iterator_t> &inbegin,
  const std::vector<mapper::const_iterator_t> &inend,
  const std::vector<reducer::inserter_t> &outbegin)
{
  auto incur = inbegin;
  const auto mapn = inbegin.size();
  const auto redn = outbegin.size();
  std::priority_queue<std::pair<mapper::value_t, unsigned>,
                      std::vector<std::pair<mapper::value_t,unsigned>>,
                      std::greater<>> heads;

  for(unsigned ii = 0; ii < mapn; ++ii) {
    if(incur[ii] != inend[ii]) {
      heads.emplace(*(incur[ii]),ii);
    }
  }

    // caclulate approximate output container size
  const auto outblocksize = insize / redn;
  auto outcur = outbegin;
  unsigned curoutblock = 0;
  unsigned long curoutsize = 0;
//  char prev_first_char = 0;
  while(!heads.empty()) {
    auto const &topval = heads.top();
    *(outcur[curoutblock]) = topval.first;
    outcur[curoutblock]++;
    curoutsize++;
      // start new block if we have at least outblocksize
      // and we are not out of blocks
    if(curoutsize >= outblocksize && (curoutblock+1) < redn) {
      curoutblock++;
      curoutsize = 1;
        // duplicate last entry from previous block in next block
      *(outcur[curoutblock]) = topval.first;
      outcur[curoutblock]++;
    }
    auto initidx = topval.second;
    heads.pop();
    incur[initidx]++;
    if(incur[initidx] != inend[initidx])
    {
      heads.emplace(*(incur[initidx]), initidx);
    }
  }
}


void maxprefixreducer(int idx, reducer::const_iterator_t cbegin, reducer::const_iterator_t cend) {
  std::ofstream ofs("rezult_"+std::to_string(idx), std::ios_base::out|std::ios::binary);
  if(!ofs.is_open()) return;
  std::string prevstr;
  unsigned curprefixlen = 0;
  for(auto strit = cbegin; strit != cend; ++strit)
  {
    const auto &curstr = *strit;
    auto oldprevstr = prevstr;
    if(curstr.length() < curprefixlen) {
      continue;
    }
    auto prevprefix = std::string_view(prevstr.data(), curprefixlen);
    auto curprefix  = std::string_view(curstr .data(), curprefixlen);
    if(prevprefix != curprefix) {
      prevstr = curstr;
      continue;
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
    if(oldprefixlen != curprefixlen)
    {
      std::cout << "new prefixlen " << curprefixlen << " prev: " << oldprevstr << " cur : " << curstr
                << "\n";
    }
  }
  std::cout << "curprefixlen: " << curprefixlen << "\n";
};


  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers,cppcoreguidelines-pro-bounds-pointer-arithmetic)
int main(int argc, char const * argv[])
{
  try
  {
    if(argc != 4)
    {
      std::cout << "Usage:\n" << argv[0] << " <filename> <map_number> <reduce_number>\n"
                << "For example:\n" << argv[0] << " email.txt 20 24\n";
      return 0;
    }

    const unsigned mapn = std::atoi(argv[2]);
    const unsigned redn = std::atoi(argv[3]);

    std::string fname = argv[1];
    if(!boost::filesystem::is_regular_file(fname)) throw std::runtime_error("input is not a regular file");

    if(mapn < 1) throw std::runtime_error("map number must be >=1");
    if(redn < 1) throw std::runtime_error("reduce number must be >=1");
    if(mapn > maxmapn) throw std::runtime_error("map number must be <= " + std::to_string(maxmapn));
    if(redn > maxredn) throw std::runtime_error("reduce number must be <= " + std::to_string(maxredn));

    MapFileLoader fl(fname, mapn);

    auto loader = [ &fl ](int blocknumber, mapper::inserter_t iit) { fl.load(blocknumber, iit); };

    auto action = [](mapper::value_t&str) { boost::to_upper(str);};

    auto sorter = [](mapper::iterator_t begin, mapper::iterator_t end) { std::sort(begin, end);};


    mapreduce mr
      {mapn, loader, action, sorter,
       shuffle,
       redn, maxprefixreducer};
    mr.run();
  }
  catch(const std::exception &e)
  {
    std::cerr << e.what() << std::endl;
    return generic_errorcode;
  }
  return 0;
}
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers,cppcoreguidelines-pro-bounds-pointer-arithmetic)
