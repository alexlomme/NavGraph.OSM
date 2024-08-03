#pragma once

#include <cmath>
#include <vector>

const double EARTH_RADIUS = 6370.986884258304;

double degToRad(double deg);

double geopointsDistance(std::pair<double, double> source,
                         std::pair<double, double> target);

double wayCost(std::vector<std::pair<double, double>> locations);