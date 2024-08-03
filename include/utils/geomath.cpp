#include "geomath.hpp"

constexpr double RAD = M_PI / 180;

double degToRad(double deg) { return deg * RAD; }

double geopointsDistance(std::pair<double, double> source,
                         std::pair<double, double> target) {
  double sourceLat = degToRad(source.first);
  double sourceLon = degToRad(source.second);
  double targetLat = degToRad(target.first);
  double targetLon = degToRad(target.second);

  double diffLat = targetLat - sourceLat;
  double diffLon = targetLon - sourceLon;
  double a = pow(sin(diffLat / 2), 2) +
             cos(sourceLat) * cos(targetLat) * pow(sin(diffLon / 2), 2);
  double c = 2 * atan2(sqrt(a), sqrt(1 - a));
  double ans = c * EARTH_RADIUS;
  return ans;
}

double wayCost(std::vector<std::pair<double, double>> locations) {
  auto source = *(locations.begin());
  std::pair<double, double> target;

  double cost = 0;

  if (locations.size() < 2) {
    return cost;
  }

  for (auto loc : locations) {
    if (loc == *(locations.begin())) {
      continue;
    }

    target = loc;

    cost += geopointsDistance(source, target);

    source = loc;
  }

  return cost;
}