/**
 * @file gtest_mapreduce.cpp
 *
 * @brief Test mapreduce
 *  */

#include "mapreduce.h"
#include "mapfileloader.h"
#include "minprefix.h"

#include <boost/filesystem/operations.hpp>
#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <sstream>

namespace {

   // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

  auto file_contents(const std::string &fname) {
    std::ifstream fit(fname);
    return std::string((std::istreambuf_iterator<char>(fit)),
                       std::istreambuf_iterator<char>());
  }

  void delete_all()
  {
    boost::filesystem::remove("rezult_0");
    boost::filesystem::remove("rezult_1");
    boost::filesystem::remove("rezult_2");
    boost::filesystem::remove("rezult_3");
    boost::filesystem::remove("rezult_4");
    boost::filesystem::remove("rezult_5");
    boost::filesystem::remove("rezult_6");
    boost::filesystem::remove("rezult_7");
    boost::filesystem::remove("rezult_8");
    boost::filesystem::remove("rezult_9");
    boost::filesystem::remove("rezult_10");
    boost::filesystem::remove("rezult_11");
    boost::filesystem::remove("rezult_12");
    boost::filesystem::remove("rezult_13");
    boost::filesystem::remove("rezult_14");
  }



  TEST(bayan, testfiles) {
    {
      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt",  1,  1), 27);
      EXPECT_EQ(file_contents("rezult_0"), "27\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt",  2,  1), 27);
      EXPECT_EQ(file_contents("rezult_0"), "27\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt",  1,  2), 27);
      EXPECT_EQ(file_contents("rezult_0"), "25\n");
      EXPECT_EQ(file_contents("rezult_1"), "27\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt",  2,  2), 27);
      EXPECT_EQ(file_contents("rezult_0"), "25\n");
      EXPECT_EQ(file_contents("rezult_1"), "27\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt", 13,  1), 27);
      EXPECT_EQ(file_contents("rezult_0"), "27\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt",  1, 13), 27);
      EXPECT_EQ(file_contents("rezult_0"), "25\n");
      EXPECT_EQ(file_contents("rezult_1"), "21\n");
      EXPECT_EQ(file_contents("rezult_2"), "23\n");
      EXPECT_EQ(file_contents("rezult_3"), "19\n");
      EXPECT_EQ(file_contents("rezult_4"), "21\n");
      EXPECT_EQ(file_contents("rezult_5"), "18\n");
      EXPECT_EQ(file_contents("rezult_6"), "22\n");
      EXPECT_EQ(file_contents("rezult_7"), "27\n");
      EXPECT_EQ(file_contents("rezult_8"), "21\n");
      EXPECT_EQ(file_contents("rezult_9"), "21\n");
      EXPECT_EQ(file_contents("rezult_10"), "20\n");
      EXPECT_EQ(file_contents("rezult_11"), "22\n");
      EXPECT_EQ(file_contents("rezult_12"), "20\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt", 13, 14), 27);
      EXPECT_EQ(file_contents("rezult_0"), "25\n");
      EXPECT_EQ(file_contents("rezult_1"), "21\n");
      EXPECT_EQ(file_contents("rezult_2"), "23\n");
      EXPECT_EQ(file_contents("rezult_3"), "19\n");
      EXPECT_EQ(file_contents("rezult_4"), "21\n");
      EXPECT_EQ(file_contents("rezult_5"), "18\n");
      EXPECT_EQ(file_contents("rezult_6"), "19\n");
      EXPECT_EQ(file_contents("rezult_7"), "27\n");
      EXPECT_EQ(file_contents("rezult_8"), "20\n");
      EXPECT_EQ(file_contents("rezult_9"), "21\n");
      EXPECT_EQ(file_contents("rezult_10"), "21\n");
      EXPECT_EQ(file_contents("rezult_11"), "20\n");
      EXPECT_EQ(file_contents("rezult_12"), "22\n");
      EXPECT_EQ(file_contents("rezult_13"), "20\n");

      delete_all();
      EXPECT_EQ(minprefix("generated-emails-10000.txt",  9,  3), 27);
      EXPECT_EQ(file_contents("rezult_0"), "25\n");
      EXPECT_EQ(file_contents("rezult_1"), "27\n");
      EXPECT_EQ(file_contents("rezult_2"), "22\n");
    }
  }

    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}
