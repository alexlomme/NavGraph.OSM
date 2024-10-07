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

double findDistance(std::pair<double, double> p1,
                    std::pair<double, double> p2) {
  double dx = p1.second - p2.second;
  double dy = p1.first - p2.first;
  return sqrt(dx * dx + dy * dy);
}

std::pair<double, double> pointOnSegmentByFraction(std::pair<double, double> p1,
                                                   std::pair<double, double> p2,
                                                   double fraction,
                                                   double distance) {
  double lat = (1 - fraction) * p1.first + (fraction * p2.first);
  double lon = (1 - fraction) * p1.second + (fraction * p2.second);
  return std::make_pair(lat, lon);
}

double getLength(std::vector<std::pair<double, double>>& line) {
  double length = 0;
  if (line.size() < 2) {
    return length;
  }

  for (int i = 1; i < line.size(); i++) {
    length += findDistance(line[i - 1], line[i]);
  }

  return length;
}

std::pair<int, std::pair<double, double>> findMiddlePoint(
    std::vector<std::pair<double, double>>& line) {
  double euclideanLength = getLength(line);
  double halfDistance = euclideanLength / 2;
  double cl = 0;
  double ol = 0;
  std::pair<double, double> result;
  int idx;
  for (int i = 1; i < line.size(); i++) {
    ol = cl;
    double tmpDist = findDistance(line[i - 1], line[i]);
    cl += tmpDist;
    if (halfDistance <= cl && halfDistance > ol) {
      double halfSub = halfDistance - ol;
      result = pointOnSegmentByFraction(line[i - 1], line[i], halfSub / tmpDist,
                                        halfSub);
      idx = i - 1;
    }
  }
  return std::make_pair(idx, result);
}