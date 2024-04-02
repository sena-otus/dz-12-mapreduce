#pragma once

#include "paralleltask.h"
#include <functional>
#include <iterator>
#include <list>
#include <string>
#include <vector>

/**
 *  @brief map stage
 *  */
class mapper : public ParallelTask {
public:
  using value_t = std::string;
  using data_t = std::vector<value_t>;
  using inserter_t = std::back_insert_iterator<data_t>;
  using const_iterator_t = typename data_t::const_iterator;
  using iterator_t = typename data_t::iterator;
  using loader_t = std::function<void(int, inserter_t)>;
  using action_t = std::function<void(value_t &)>;
  using sorter_t = std::function<void(iterator_t, iterator_t)>;

    /**
     *  @param idx mapper number
     *  @param loader should load mapper containers
     *  @param action can modify loaded values (can be nullptr)
     *  @param sorter sorts mapper conainer (can be nullptr)
     *  */
  explicit mapper(int idx, loader_t &loader, action_t &action, sorter_t &sorter)
    : m_idx(idx), m_loader(loader), m_action(action), m_sorter(sorter) {}

  void doit() override {
    m_loader(m_idx, backinserter());
    if(m_action) std::for_each(begin(), end(), m_action);
    if(m_sorter) m_sorter(begin(), end());
  }

  [[nodiscard]] size_t size() const { return m_data.size(); }
  [[nodiscard]] inserter_t backinserter() { return std::back_insert_iterator<data_t>(m_data); }
  [[nodiscard]] iterator_t begin() { return m_data.begin(); }
  [[nodiscard]] iterator_t end  () { return m_data.end(); }
  [[nodiscard]] const_iterator_t cbegin() const { return m_data.cbegin(); }
  [[nodiscard]] const_iterator_t cend() const { return m_data.cend(); }

private:
  int m_idx;
  data_t m_data;
  loader_t m_loader;
  action_t m_action;
  sorter_t m_sorter;
};

/** @brief reduce stage */
class reducer : public ParallelTask {
public:
  using value_t = std::string;
  using data_t = std::vector<value_t>;
  using inserter_t = std::back_insert_iterator<data_t>;
  using const_iterator_t = data_t::const_iterator;
  using payload_t = std::function<void(int, const_iterator_t, const_iterator_t)>;

    /**
     *  @param idx reducer number
     *  @param pl useful payload should process container and create output files
     *  */
  explicit reducer(int idx, payload_t &pl);
  void doit() override { m_pl(m_idx, m_data.cbegin(), m_data.cend()); }

  inserter_t backinserter() {
    return std::back_insert_iterator<data_t>(m_data);
  }
  [[nodiscard]] const_iterator_t cbegin() const { return m_data.cbegin(); }
  [[nodiscard]] const_iterator_t cend  () const { return m_data.cend(); }

private:
  int m_idx;
  payload_t m_pl;
  data_t m_data;
};

/**
 *  @brief map-reduce framework
 *  */
class mapreduce {
public:
  /**
   *  @brief should shuffle rezults from mapper into reducer containers
   *  - whole amount of input lines (calculated on mapper stage)
   *  - array of begin mapper iterators (there must be mapn iterators)
   *  - array of end mapper iterators (there must be mapn iterators)
   *  - array of reducer container iterators (there must be redn iterators)
   *  */
  using shuffler_t = std::function<void(size_t,
    const std::vector<mapper::const_iterator_t> &,
    const std::vector<mapper::const_iterator_t> &,
    const std::vector<reducer::inserter_t> &)>;

    /**
     *  @brief ctor setup all customizers
     *  @param mapn amount of mapper threads
     *  @param maploader should load mapper containers
     *  @param mapaction modify loaded values (can be nullptr)
     *  @param mapsorter sorts mapper conainer (can be nullptr)
     *  @param shuffler moves items from mapper to reducer containers
     *  @param redn amount of reducer threads
     *  @param rpl reducer payload, should also create output files
     *  */
  mapreduce(unsigned mapn, mapper::loader_t maploader, mapper::action_t mapaction, mapper::sorter_t mapsorter,
            shuffler_t shuffler,
            unsigned redn, reducer::payload_t rpl);
    /** size of all mapper containers */
  [[nodiscard]] size_t mapsize() const;
    /** run map-reduce */
  void run();

private:
  shuffler_t m_shuffler;
  std::list<mapper> m_maps;
  std::list<reducer> m_reds;
};
