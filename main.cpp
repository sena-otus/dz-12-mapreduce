/**
 * @file main.cpp
 * @brief Exercise 12, mapreduce
 *  */

#include "mapreduce.h"
#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <ios>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <regex>
#include <utility>

//namespace po = boost::program_options;

const int generic_errorcode = 102;



  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
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

    mapreduce mr(argv[1], mapn, redn);
  }
  catch(const std::exception &e)
  {
    std::cerr << e.what() << std::endl;
    return generic_errorcode;
  }
  return 0;
}
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
