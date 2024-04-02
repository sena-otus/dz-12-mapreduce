#pragma once

#include <functional>
#include <iterator>
#include <list>
#include <string>
#include <thread>
#include <vector>

class ParallelTask {
public:
  ParallelTask() = default;
  ParallelTask(ParallelTask &&other) = default;
  ParallelTask(const ParallelTask &other) = delete;
  ParallelTask &operator=(ParallelTask &&other) = default;
  ParallelTask &operator=(const ParallelTask &other) = delete;
  virtual ~ParallelTask() = default;
  virtual void doit() = 0;

  void start_parallel() { m_th = std::thread(&ParallelTask::doit, this); }
  void join() { m_th.join(); }

private:
  std::thread m_th;
};

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

  explicit mapper(int idx, loader_t &loader, action_t &action, sorter_t &sorter)
    : m_idx(idx), m_loader(loader), m_action(action), m_sorter(sorter) {}

  void doit() override {
    m_loader(m_idx, backinserter());
    if(m_action) std::for_each(begin(), end(), m_action);
    if(m_sorter) m_sorter(begin(), end());
  }

  [[nodiscard]] size_t osize() const { return m_data.size(); }
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

class reducer : public ParallelTask {
public:
  using value_t = std::string;
  using data_t = std::vector<value_t>;
  using inserter_t = std::back_insert_iterator<data_t>;
  using const_iterator_t = data_t::const_iterator;
  using payload_t = std::function<void(int, const_iterator_t, const_iterator_t)>;

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

class mapreduce {
public:
  using shuffler_t = std::function<void(size_t,
    const std::vector<mapper::const_iterator_t> &,
    const std::vector<mapper::const_iterator_t> &,
    const std::vector<reducer::inserter_t> &)>;

  mapreduce(unsigned mapn, mapper::loader_t maploader, mapper::action_t mapaction, mapper::sorter_t mapsorter,
            shuffler_t shuffler,
            unsigned redn, reducer::payload_t rpl);
  [[nodiscard]] size_t size() const;
  void run();

private:
  shuffler_t m_shuffler;
  std::list<mapper> m_maps;
  std::list<reducer> m_reds;
};
