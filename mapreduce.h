#pragma once

#include <iterator>
#include <string>
#include <thread>
#include <list>
#include <vector>

class mapper
{
public:
  using out_elem_t = std::string;
  using out_t = std::vector<out_elem_t>;
  using out_const_iterator_t = out_t::const_iterator;
  mapper(std::string fname, long offset, long blocksize);
  void domap();
  void start_parallel();
  void join();
  [[nodiscard]] size_t osize() const;

  [[nodiscard]] out_const_iterator_t outbegin() const  { return m_data.cbegin();}
  [[nodiscard]] out_const_iterator_t outend  () const  { return m_data.cend();}

private:
  std::string m_fname;
  long m_offset;
  long m_blocksize;
  out_t m_data;
  std::thread m_th;
};

class reducer
{
public:
  using in_elem_t = std::string;
  using in_cont_t = std::vector<in_elem_t>;
  using in_inserter_t = std::back_insert_iterator<in_cont_t>;

  in_inserter_t backinserter() {
    return std::back_insert_iterator<in_cont_t>(m_in);
  }

private:
  in_cont_t m_in;
};




class mapreduce
{
public:
  mapreduce(std::string fname, unsigned mapn, unsigned redn);
  [[nodiscard]] size_t size() const;
private:
  std::string m_fname;
  unsigned m_mapn;
  unsigned m_redn;
  std::list<mapper> m_maps;
  std::list<reducer> m_reds;
};