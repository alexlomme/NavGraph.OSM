#include <fstream>
#include <handler.hpp>
#include <iostream>
#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "No input file passed" << std::endl;
        return 1;
    }

    std::string filename = argv[1];

    osmium::io::File input{filename};
    osmium::io::Reader reader{input};

    CustomHandler handler;

    osmium::apply(reader, handler);

    reader.close();

    try {
        auto tuple = handler.convertDataToGraph();

        auto edges = std::get<1>(tuple);

        std::string outputFile = argc > 2 ? argv[2] : "output.csv";

        std::ofstream file(outputFile);

        if (!file.is_open()) {
            std::cerr << "Error opening file" << std::endl;
            return 1;
        }

        for (auto& pair : edges) {
            auto edge = pair.second;
            auto source = edge.sourceNode.location;
            auto target = edge.targetNode.location;

            std::ostringstream sourceStream;
            sourceStream << "(" << source.lon() << " " << source.lat() << ")";

            std::ostringstream targetStream;
            targetStream << "(" << target.lon() << " " << target.lat() << ")";

            file << sourceStream.str() << "," << targetStream.str() << ","
                 << edge.cost << std::endl;
        }

        file.close();
    } catch (std::runtime_error exc) {
        std::cerr << exc.what() << std::endl;
    }

    return 0;
}