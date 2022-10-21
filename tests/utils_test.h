#pragma once
#include <catch2/catch.hpp>
#include <Utils.h>
#include <iostream>


TEST_CASE("Get Home Directory Test")
{
    using namespace bluefin::chicago::applications;

    auto homedir = Utils::get_homedir();
    REQUIRE_FALSE(homedir.empty());
}

TEST_CASE("Test Char Algorithm")
{
    std::string s1("A");
    int next = (s1[0]-'A') + 1;
    next %= 10;
    s1[0] = 'A' + next;
    REQUIRE(s1 == "B");

    std::string s2("I");
    next = (s2[0]-'A') + 1;
    next %= 10;
    s2[0] = 'A' + next;
    REQUIRE(s2 == "J");

    std::string s3("J");
    next = (s3[0]-'A') + 1;
    next %= 10;
    s3[0] = 'A' + next;
    REQUIRE(s3 == "A");
}