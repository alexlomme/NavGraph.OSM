#include <cmath>
#include <osmium/osm/location.hpp>

const double EARTH_RADIUS = 6370.986884258304;

constexpr double degToRad(double deg) { return deg * M_PI / 180; }

double geopointsDistance(osmium::Location source, osmium::Location target) {
    double sourceLat = degToRad(source.lat());
    double sourceLon = degToRad(source.lon());
    double targetLat = degToRad(target.lat());
    double targetLon = degToRad(target.lon());

    double diffLat = targetLat - sourceLat;
    double diffLon = targetLon - sourceLon;
    double a = pow(sin(diffLat / 2), 2) +
               cos(sourceLat) * cos(targetLat) * pow(sin(diffLon / 2), 2);
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));
    double ans = c * EARTH_RADIUS;
    return ans;
}

double wayCost(std::vector<osmium::Location> locations) {
    auto source = *(locations.begin());
    osmium::Location target;

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