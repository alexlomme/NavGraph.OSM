#pragma once

#include <fcntl.h>
#include <tbb/concurrent_unordered_set.h>

#include <cstdlib>
#include <disk/file-read.hpp>
#include <disk/utils.hpp>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <utils/geomath.hpp>

namespace ngosm {
namespace csv {
void generate(tbb::concurrent_unordered_set<long>& usedPixels,
              std::string output) {
  char path[PATH_MAX];

  realpath(".", path);

  std::ofstream ofile(path + std::string("/") + output);

  if (!ofile.is_open()) {
    throw std::runtime_error("Error opening file");
  }

  // from_vertex_id;
  // to_vertex_id;
  // weight;
  // geom;
  // was_one_way;
  // edge_id;
  // osm_way_from;
  // osm_way_to;
  // osm_way_from_source_node;
  // osm_way_from_target_node;
  // osm_way_to_source_node;
  // osm_way_to_target_node

  ofile
      // << "from_vertex_id,"
      // << "to_vertex_id,"
      << "weight,"
      << "geom,"
      << "was_one_way,"
      // << "edge_id,"
      << "osm_way_from,"
      << "osm_way_to,"
      << "osm_way_from_source_node,"
      << "osm_way_from_target_node,"
      << "osm_way_to_source_node,"
      << "osm_way_to_target_node" << std::endl;

  for (auto ipix : usedPixels) {
    int fd_e =
        open(numFilename(path + std::string("/bin/geo-partitions/edges/edges-"),
                         ipix, "bin")
                 .data(),
             O_RDONLY);
    unlink(numFilename(path + std::string("/bin/geo-partitions/edges/edges-"),
                       ipix, "bin")
               .data());
    int fd_be =
        open(numFilename(
                 path + std::string("/bin/geo-partitions/border-edges/edges-"),
                 ipix, "bin")
                 .data(),
             O_RDONLY);

    int fd_ee =
        open(numFilename(
                 path + std::string("/bin/geo-partitions/exp-edges/exp-edges-"),
                 ipix, "bin")
                 .data(),
             O_RDONLY);
    unlink(numFilename(
               path + std::string("/bin/geo-partitions/exp-edges/exp-edges-"),
               ipix, "bin")
               .data());

    int fd_eg =
        open(numFilename(
                 path + std::string("/bin/geo-partitions/edge-geometry/geo-"),
                 ipix, "bin")
                 .data(),
             O_RDONLY);
    unlink(numFilename(
               path + std::string("/bin/geo-partitions/edge-geometry/geo-"),
               ipix, "bin")
               .data());
    int fd_beg = open(
        numFilename(
            path + std::string("/bin/geo-partitions/border-edge-geometry/geo-"),
            ipix, "bin")
            .data(),
        O_RDONLY);

    FileRead edgesFileRead(fd_e);
    FileRead borderEdgesFileRead(fd_be);
    FileRead expEdgesFileRead(fd_ee);
    FileRead edgeGeomFileRead(fd_eg);
    FileRead borderEdgeGeomFileRead(fd_beg);

    void* map_e = edgesFileRead.mmap_file();
    ngosm::types::Edge* edges =
        reinterpret_cast<ngosm::types::Edge*>(static_cast<char*>(map_e));
    void* map_be = borderEdgesFileRead.mmap_file();
    ngosm::types::Edge* borderEdges =
        reinterpret_cast<ngosm::types::Edge*>(static_cast<char*>(map_be));
    void* map_ee = expEdgesFileRead.mmap_file();
    void* map_eg = edgeGeomFileRead.mmap_file();
    std::pair<double, double>* edgeGeom =
        reinterpret_cast<std::pair<double, double>*>(
            static_cast<char*>(map_eg));
    void* map_beg = borderEdgeGeomFileRead.mmap_file();
    std::pair<double, double>* borderEdgeGeom =
        reinterpret_cast<std::pair<double, double>*>(
            static_cast<char*>(map_beg));

    uint64_t eeCount =
        expEdgesFileRead.fsize() / sizeof(ngosm::types::ExpandedEdge);

    uint64_t beCount = borderEdgesFileRead.fsize() / sizeof(ngosm::types::Edge);

    for (uint64_t i = 0; i < eeCount; i++) {
      ngosm::types::ExpandedEdge* expEdge =
          reinterpret_cast<ngosm::types::ExpandedEdge*>(
              static_cast<char*>(map_ee) +
              i * sizeof(ngosm::types::ExpandedEdge));

      ngosm::types::Edge* sourceEdge;

      if (expEdge->sourcePart > 0) {
        sourceEdge = edges + expEdge->sourceEdgeOffset;
      } else {
        sourceEdge = borderEdges + expEdge->sourceEdgeOffset;
      }

      ngosm::types::Edge* targetEdge;

      if (expEdge->targetPart > 0) {
        targetEdge = edges + expEdge->targetEdgeOffset;
      } else {
        targetEdge = borderEdges + expEdge->targetEdgeOffset;
      }

      // std::cerr << "before" << std::endl;

      // std::cerr << expEdge->sourcePart << std::endl;
      // std::cerr << expEdge->targetEdgeOffset << std::endl;

      auto from_vertex_id = sourceEdge->id;
      auto to_vertex_id = targetEdge->id;
      auto weight = expEdge->cost;

      // std::cerr << "after" << std::endl;

      std::pair<double, double>* sourceGeom;

      if (sourceEdge->part > 0) {
        sourceGeom = edgeGeom + sourceEdge->geomOffset;
      } else {
        // std::cerr << "offset: " << sourceEdge->geomOffset << std::endl;
        sourceGeom = borderEdgeGeom + sourceEdge->geomOffset;
      }

      std::vector<std::pair<double, double>> sourceGeomList;

      // std::cerr << "before source geom: " << i << std::endl;

      if (sourceEdge->geomSize > 0) {
        for (uint64_t j = 0; j < sourceEdge->geomSize; j++) {
          sourceGeomList.push_back(*(sourceGeom + j));
        }
      } else {
        for (int64_t j = -sourceEdge->geomSize - 1; j >= 0; j--) {
          sourceGeomList.push_back(*(sourceGeom + j));
        }
      }

      // // std::cerr << "after target geom" << std::endl;

      auto midPoint = findMiddlePoint(sourceGeomList);

      std::vector<std::pair<double, double>> firstHalfGeom;
      firstHalfGeom.push_back(midPoint.second);

      for (uint64_t j = midPoint.first + 1; j < sourceGeomList.size(); j++) {
        firstHalfGeom.push_back(sourceGeomList[j]);
      }

      std::pair<double, double>* targetGeom;

      if (targetEdge->part > 0) {
        targetGeom = edgeGeom + targetEdge->geomOffset;
      } else {
        targetGeom = borderEdgeGeom + targetEdge->geomOffset;
      }

      std::vector<std::pair<double, double>> targetGeomList;

      if (targetEdge->geomSize > 0) {
        for (uint64_t j = 0; j < targetEdge->geomSize; j++) {
          targetGeomList.push_back(*(targetGeom + j));
        }
      } else {
        for (int64_t j = -targetEdge->geomSize - 1; j >= 0; j--) {
          targetGeomList.push_back(*(targetGeom + j));
        }
      }

      auto midPointTarget = findMiddlePoint(targetGeomList);

      std::vector<std::pair<double, double>> secondHalfGeom;

      for (uint64_t j = 0; j <= midPointTarget.first; j++) {
        secondHalfGeom.push_back(targetGeomList[j]);
      }

      secondHalfGeom.push_back(midPointTarget.second);

      auto was_one_way = sourceEdge->wasOneWay;
      auto edge_id = expEdge->id;
      auto osm_way_from = sourceEdge->wayId;
      auto osm_way_to = targetEdge->wayId;

      auto osm_way_from_source_node = sourceEdge->sourceNodeId;
      auto osm_way_from_target_node = sourceEdge->targetNodeId;
      auto osm_way_to_source_node = targetEdge->sourceNodeId;
      auto osm_way_to_target_node = targetEdge->targetNodeId;

      // ofile << from_vertex_id << ",";
      // ofile << to_vertex_id << ",";
      ofile << std::fixed << std::setprecision(6) << weight << ",";
      ofile << "LINESTRING(";

      for (uint64_t j = 0; j < firstHalfGeom.size(); j++) {
        ofile << std::fixed << std::setprecision(6) << firstHalfGeom[j].second
              << " " << firstHalfGeom[j].first << ";";
      }
      for (uint64_t j = 0; j < secondHalfGeom.size(); j++) {
        ofile << std::fixed << std::setprecision(6) << secondHalfGeom[j].second
              << " " << secondHalfGeom[j].first;
        if (j != secondHalfGeom.size() - 1) {
          ofile << ";";
        }
      }

      ofile << ")"
            << ",";

      ofile << (was_one_way ? "true" : "false") << ",";
      // ofile << edge_id << ",";
      ofile << osm_way_from << ",";
      ofile << osm_way_to << ",";
      ofile << osm_way_from_source_node << ",";
      ofile << osm_way_from_target_node << ",";
      ofile << osm_way_to_source_node << ",";
      ofile << osm_way_to_target_node << std::endl;
    }

    edgesFileRead.unmap_file();
    edgesFileRead.close_fd();

    borderEdgesFileRead.unmap_file();
    borderEdgesFileRead.close_fd();

    expEdgesFileRead.unmap_file();
    expEdgesFileRead.close_fd();

    edgeGeomFileRead.unmap_file();
    edgeGeomFileRead.close_fd();

    borderEdgeGeomFileRead.unmap_file();
    borderEdgeGeomFileRead.close_fd();
  }

  int fd_bee = open(
      (path + std::string("/bin/geo-partitions/exp-edges/border-exp-edges.bin"))
          .data(),
      O_RDONLY);

  FileRead borderExpEdgesRead(fd_bee);

  void* map_bee = borderExpEdgesRead.mmap_file();

  uint64_t beeCount =
      borderExpEdgesRead.fsize() / sizeof(ngosm::types::ExpandedEdge);

  std::unordered_map<long, std::vector<ngosm::types::ExpandedEdge*>> beeTab;

  for (uint64_t i = 0; i < beeCount; i++) {
    ngosm::types::ExpandedEdge* expEdge =
        reinterpret_cast<ngosm::types::ExpandedEdge*>(
            static_cast<char*>(map_bee) +
            i * sizeof(ngosm::types::ExpandedEdge));

    long ipix = -expEdge->sourcePart;

    auto it = beeTab.find(ipix);

    if (it == beeTab.end()) {
      it = beeTab
               .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                        std::forward_as_tuple())
               .first;
      it->second.push_back(expEdge);
    } else {
      it->second.push_back(expEdge);
    }
  }

  for (const auto& [ipix, buf] : beeTab) {
    int fd_be_source =
        open(numFilename(
                 path + std::string("/bin/geo-partitions/border-edges/edges-"),
                 ipix, "bin")
                 .data(),
             O_RDONLY);
    FileRead borderEdgesSourceRead(fd_be_source);

    void* map_be_source = borderEdgesSourceRead.mmap_file();

    ngosm::types::Edge* borderEdgesSource =
        reinterpret_cast<ngosm::types::Edge*>(
            static_cast<char*>(map_be_source));

    int fd_beg_source = open(
        numFilename(
            path + std::string("/bin/geo-partitions/border-edge-geometry/geo-"),
            ipix, "bin")
            .data(),
        O_RDONLY);
    FileRead borderEdgeGeomSourceRead(fd_beg_source);

    void* map_beg_source = borderEdgeGeomSourceRead.mmap_file();

    std::pair<double, double>* borderEdgeGeoms =
        reinterpret_cast<std::pair<double, double>*>(
            static_cast<char*>(map_beg_source));

    std::unordered_map<long, std::vector<ngosm::types::ExpandedEdge*>>
        expEdgesByTargetIpix;

    for (auto expEdgePtr : buf) {
      long targetIpix = -expEdgePtr->targetPart;

      auto it = expEdgesByTargetIpix.find(targetIpix);

      if (it == expEdgesByTargetIpix.end()) {
        it = expEdgesByTargetIpix
                 .emplace(std::piecewise_construct,
                          std::forward_as_tuple(targetIpix),
                          std::forward_as_tuple())
                 .first;
      }

      it->second.push_back(expEdgePtr);
    }

    for (const auto& [targetIpix, targetBuf] : expEdgesByTargetIpix) {
      int fd_be_target = open(
          numFilename(
              path + std::string("/bin/geo-partitions/border-edges/edges-"),
              targetIpix, "bin")
              .data(),
          O_RDONLY);
      FileRead borderEdgesTargetRead(fd_be_target);
      void* map_be_target = borderEdgesTargetRead.mmap_file();
      ngosm::types::Edge* borderEdgesTarget =
          reinterpret_cast<ngosm::types::Edge*>(
              static_cast<char*>(map_be_target));

      int fd_beg_target = open(
          numFilename(
              path +
                  std::string("/bin/geo-partitions/border-edge-geometry/geo-"),
              targetIpix, "bin")
              .data(),
          O_RDONLY);
      FileRead borderEdgeGeomTargetRead(fd_beg_target);
      void* map_beg_target = borderEdgeGeomTargetRead.mmap_file();
      std::pair<double, double>* borderEdgeGeomsTarget =
          reinterpret_cast<std::pair<double, double>*>(
              static_cast<char*>(map_beg_target));

      for (auto ee : targetBuf) {
        ngosm::types::Edge* sourceEdge =
            borderEdgesSource + ee->sourceEdgeOffset;

        ngosm::types::Edge* targetEdge =
            borderEdgesTarget + ee->targetEdgeOffset;

        auto from_vertex_id = sourceEdge->id;
        auto to_vertex_id = targetEdge->id;
        auto weight = ee->cost;

        std::pair<double, double>* sourceGeom =
            borderEdgeGeoms + sourceEdge->geomOffset;

        std::vector<std::pair<double, double>> sourceGeomList;

        if (sourceEdge->geomSize > 0) {
          for (uint64_t j = 0; j < sourceEdge->geomSize; j++) {
            sourceGeomList.push_back(*(sourceGeom + j));
          }
        } else {
          for (int64_t j = -sourceEdge->geomSize - 1; j >= 0; j--) {
            sourceGeomList.push_back(*(sourceGeom + j));
          }
        }

        auto midPoint = findMiddlePoint(sourceGeomList);

        std::vector<std::pair<double, double>> firstHalfGeom;
        firstHalfGeom.push_back(midPoint.second);

        for (uint64_t j = midPoint.first + 1; j < sourceGeomList.size(); j++) {
          firstHalfGeom.push_back(sourceGeomList[j]);
        }

        std::pair<double, double>* targetGeom =
            borderEdgeGeomsTarget + targetEdge->geomOffset;

        std::vector<std::pair<double, double>> targetGeomList;

        if (targetEdge->geomSize > 0) {
          for (uint64_t j = 0; j < targetEdge->geomSize; j++) {
            targetGeomList.push_back(*(targetGeom + j));
          }
        } else {
          for (int64_t j = -targetEdge->geomSize - 1; j >= 0; j--) {
            targetGeomList.push_back(*(targetGeom + j));
          }
        }

        auto midPointTarget = findMiddlePoint(targetGeomList);

        std::vector<std::pair<double, double>> secondHalfGeom;

        for (uint64_t j = 0; j <= midPointTarget.first; j++) {
          secondHalfGeom.push_back(targetGeomList[j]);
        }

        secondHalfGeom.push_back(midPointTarget.second);

        auto was_one_way = sourceEdge->wasOneWay;
        auto edge_id = ee->id;
        auto osm_way_from = sourceEdge->wayId;
        auto osm_way_to = targetEdge->wayId;

        auto osm_way_from_source_node = sourceEdge->sourceNodeId;
        auto osm_way_from_target_node = sourceEdge->targetNodeId;
        auto osm_way_to_source_node = targetEdge->sourceNodeId;
        auto osm_way_to_target_node = targetEdge->targetNodeId;

        // ofile << from_vertex_id << ",";
        // ofile << to_vertex_id << ",";
        ofile << std::fixed << std::setprecision(6) << weight << ",";
        ofile << "LINESTRING(";

        for (uint64_t j = 0; j < firstHalfGeom.size(); j++) {
          ofile << std::fixed << std::setprecision(6) << firstHalfGeom[j].second
                << " " << firstHalfGeom[j].first << ";";
        }
        for (uint64_t j = 0; j < secondHalfGeom.size(); j++) {
          ofile << std::fixed << std::setprecision(6)
                << secondHalfGeom[j].second << " " << secondHalfGeom[j].first;
          if (j != secondHalfGeom.size() - 1) {
            ofile << ";";
          }
        }

        ofile << ")"
              << ",";

        ofile << (was_one_way ? "true" : "false") << ",";
        // ofile << edge_id << ",";
        ofile << osm_way_from << ",";
        ofile << osm_way_to << ",";
        ofile << osm_way_from_source_node << ",";
        ofile << osm_way_from_target_node << ",";
        ofile << osm_way_to_source_node << ",";
        ofile << osm_way_to_target_node << std::endl;
      }

      borderEdgesTargetRead.unmap_file();
      borderEdgesTargetRead.close_fd();

      borderEdgeGeomTargetRead.unmap_file();
      borderEdgeGeomTargetRead.close_fd();
    }
    borderEdgeGeomSourceRead.unmap_file();
    borderEdgeGeomSourceRead.close_fd();

    borderEdgesSourceRead.unmap_file();
    borderEdgesSourceRead.close_fd();
  }

  for (long ipix : usedPixels) {
    int fd =
        open(numFilename(
                 path + std::string("./bin/geo-partitions/border-edges/edges-"),
                 ipix, "bin")
                 .data(),
             O_RDONLY);
    if (fd == -1) {
      continue;
    }

    unlink(numFilename(
               path + std::string("./bin/geo-partitions/border-edges/edges-"),
               ipix, "bin")
               .data());
    close(fd);
  }

  for (long ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/border-nodes/nodes-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd =
        open(numFilename(
                 path + std::string("./bin/geo-partitions/border-nodes/nodes-"),
                 ipix, "bin")
                 .data(),
             O_RDONLY);
    if (fd == -1) {
      continue;
    }
    unlink(numFilename(
               path + std::string("./bin/geo-partitions/border-nodes/nodes-"),
               ipix, "bin")
               .data());
    close(fd);
  }
}
}  // namespace csv
}  // namespace ngosm