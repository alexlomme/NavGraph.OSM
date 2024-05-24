#include <zlib.h>

#include <iostream>
#include <string>

bool decompress(std::string data, unsigned char *buf, size_t rawSize) {
    z_stream zstrm;
    zstrm.zalloc = Z_NULL;
    zstrm.zfree = Z_NULL;
    zstrm.opaque = Z_NULL;
    zstrm.avail_in = 0;
    zstrm.next_in = Z_NULL;
    if (inflateInit(&zstrm) != Z_OK) {
        std::cerr << "Unable to initialize zlib" << std::endl;
        return false;
    }

    zstrm.avail_in = data.capacity();
    zstrm.next_in = (Bytef *)data.data();
    zstrm.avail_out = rawSize;
    zstrm.next_out = buf;
    int r = inflate(&zstrm, Z_NO_FLUSH);
    if (r == Z_STREAM_ERROR) {
        std::cout << "Not OK: " << r << "\n";
        return false;
    }

    inflateEnd(&zstrm);
    return true;
}