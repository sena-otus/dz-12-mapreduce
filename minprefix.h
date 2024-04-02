#pragma once

#include <string>

/**
 * @brief calculate minimum possible unique prefix for line from text file
 * @param fname input file name
 * @param mapn amount of mapper threads
 * @param redn amount of reducer theads
 * */
extern unsigned minprefix(const std::string &fname, unsigned mapn, unsigned redn);
