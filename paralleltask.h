#pragma once

#include <thread>

/** helper class to run tasks in parallel */
class ParallelTask {
public:
  ParallelTask() = default;
  ParallelTask(ParallelTask &&other) = default;
  ParallelTask(const ParallelTask &other) = delete;
  ParallelTask &operator=(ParallelTask &&other) = default;
  ParallelTask &operator=(const ParallelTask &other) = delete;
  virtual ~ParallelTask() = default;
    /** users should override that method, it will be run in parallel */
  virtual void doit() = 0;
    /** start parallel execution */
  void start_parallel() { m_th = std::thread(&ParallelTask::doit, this); }
    /** wait until the thread is stopped */
  void join() { m_th.join(); }

private:
  std::thread m_th;
};
