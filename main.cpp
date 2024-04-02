/**
 * @file main.cpp
 * @brief Exercise 12, mapreduce
 *  */

#include "minprefix.h"

#include <iostream>
#include <stdexcept>
#include <string>


const int generic_errorcode = 1;


  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers,cppcoreguidelines-pro-bounds-pointer-arithmetic,cert-err34-c)
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
    const std::string fname = argv[1];

    std::cout << "Smallest unique prefix: " << minprefix(fname, mapn, redn) << "\n";
  }
  catch(const std::exception &e)
  {
    std::cerr << e.what() << std::endl;
    return generic_errorcode;
  }
  return 0;
}
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers,cppcoreguidelines-pro-bounds-pointer-arithmetic,cert-err34-c)
