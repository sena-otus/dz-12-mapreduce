#include "mapreduce.h"

#include <iostream>
#include <fstream>
#include <numeric>
#include <boost/filesystem.hpp>
#include <string_view>
#include <utility>


reducer::reducer(int idx, payload_t &pl)
  : m_idx(idx), m_pl(pl)
{
}



mapreduce::mapreduce(unsigned mapn,
                     mapper::loader_t maploader,
                     mapper::action_t mapaction,
                     mapper::sorter_t mapsorter,
                     mapreduce::shuffler_t shuffler,
                     unsigned redn, reducer::payload_t rpl
                     )
  : m_shuffler{std::move(shuffler)}
{
  for(unsigned ii = 0; ii < mapn; ++ii)
  {
    m_maps.emplace_back(ii, maploader, mapaction, mapsorter);
  }
  for(unsigned ii = 0; ii < redn; ii++) {
    m_reds.emplace_back(ii, rpl);
  }
}



void
mapreduce::run()
{
    // map
  for(auto && mapper : m_maps) {
    mapper.start_parallel();
  }
  for(auto && mapper : m_maps) {
    mapper.join();
  }
    // shuffle
  std::vector<mapper::const_iterator_t> mbegin;
  std::vector<mapper::const_iterator_t> mend;
  for(auto && mapper : m_maps) {
    mbegin.emplace_back(mapper.cbegin());
    mend.emplace_back(mapper.cend());
  }

  std::vector<reducer::inserter_t> ribegin;

  for(auto && red : m_reds) {
    ribegin.emplace_back(red.backinserter());
  }
  m_shuffler(size(), mbegin, mend, ribegin);

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
