#pragma once

#include <cmath>
#include <vector>

const double EARTH_RADIUS = 6370.986884258304;

double degToRad(double deg);

double geopointsDistance(std::pair<double, double> source,
                         std::pair<double, double> target);

double wayCost(std::vector<std::pair<double, double>> locations);

std::pair<int, std::pair<double, double>> findMiddlePoint(
    std::vector<std::pair<double, double>>& line);

double findDistance(std::pair<double, double> p1, std::pair<double, double> p2);

std::pair<double, double> pointOnSegmentByFraction(std::pair<double, double> p1,
                                                   std::pair<double, double> p2,
                                                   double fraction,
                                                   double distance);

double getLength(std::vector<std::pair<double, double>>& line);                                                   