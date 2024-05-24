#include <arpa/inet.h>
#include <osmpbf/fileformat.pb.h>

#include <fstream>
#include <graph.hpp>
#include <inflate.hpp>
#include <sstream>

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 2) {
        std::cerr << "No input file passed" << std::endl;
        return 1;
    }

    std::string filename = argv[1];

    std::ifstream file(filename, std::ios::in | std::ios::binary);

    if (!file.is_open()) {
        std::cerr << "Error opening input file";
        return 1;
    }

    std::vector<parser::Way> ways;
    std::unordered_map<google::protobuf::int64, parser::Node> nodes;

    while (!file.eof()) {
        uint32_t headerSize;
        file.read(reinterpret_cast<char*>(&headerSize), sizeof(headerSize));
        if (file.eof()) break;
        headerSize = ntohl(headerSize);

        std::vector<char> headerData(headerSize);
        file.read(headerData.data(), headerSize);

        OSMPBF::BlobHeader blobHeader;

        if (!blobHeader.ParseFromArray(headerData.data(), headerSize)) {
            std::cerr << "Error while reading BlobHeader" << std::endl;
            return -1;
        }

        uint32_t blobSize = blobHeader.datasize();
        std::vector<char> data(blobSize);
        file.read(data.data(), blobSize);

        OSMPBF::Blob blob;

        if (!blob.ParseFromArray(data.data(), blobSize)) {
            std::cerr << "Error while reading blob" << std::endl;
            return -1;
        }

        if (blobHeader.type() != "OSMData") {
            continue;
        }

        unsigned char* uncompressedData = new unsigned char[blob.raw_size()];

        if (blob.has_raw()) {
            const std::string rawData = blob.raw();
            std::copy(rawData.begin(), rawData.end(), uncompressedData);
        } else if (blob.has_zlib_data()) {
            decompress(blob.zlib_data(), uncompressedData, blob.raw_size());
        } else {
            std::cerr << "Unsupported compression format" << std::endl;
            return -1;
        }

        OSMPBF::PrimitiveBlock primitiveBlock;

        if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
            std::cerr << "Unable to parse primitive block" << std::endl;
            return -1;
        }

        parser::PrimitiveBlockParser parser(primitiveBlock);

        parser.parse(ways, nodes);
    }

    std::unordered_multimap<google::protobuf::int64, google::protobuf::int64>
        graph;
    std::unordered_map<google::protobuf::int64, parser::Edge> edges;

    convertToGraph(ways, nodes, &graph, &edges);

    std::string outputFile = argc > 2 ? argv[2] : "output.csv";

    std::ofstream ofile(outputFile);

    if (!ofile.is_open()) {
        std::cerr << "Error opening file" << std::endl;
        return 1;
    }

    for (auto& pair : edges) {
        auto edge = pair.second;
        auto source = edge.sourceNode;
        auto target = edge.targetNode;

        std::ostringstream sourceStream;
        sourceStream << "(" << source.lon << " " << source.lat << ")";

        std::ostringstream targetStream;
        targetStream << "(" << target.lon << " " << target.lat << ")";

        ofile << sourceStream.str() << "," << targetStream.str() << ","
              << edge.cost << std::endl;
    }

    file.close();

    return 0;
}