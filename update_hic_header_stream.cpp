// g++ -std=c++11 update_hic_header_stream.cpp -o update_hic_header_stream
// ./update_hic_header_stream input.hic output.hic key1 file1 [key2 file2 ...]

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

// Read/write little-endian helpers
static int32_t readInt32LE(const char* p) {
    int32_t v; std::memcpy(&v, p, 4); return v;
}
static int64_t readInt64LE(const char* p) {
    int64_t v; std::memcpy(&v, p, 8); return v;
}
static void writeInt32LE(char* p, int32_t v) {
    std::memcpy(p, &v, 4);
}
static void writeInt64LE(char* p, int64_t v) {
    std::memcpy(p, &v, 8);
}

int main(int argc, char** argv) {
    if (argc < 5 || ((argc - 3) % 2) != 0) {
        std::cerr << "Usage: " << argv[0]
                  << " <in.hic> <out.hic> <key1> <file1> [<key2> <file2> ...]\n";
        std::cerr << "       Each value file will be inserted as an attribute value.\n";
        return 1;
    }
    const std::string inPath  = argv[1];
    const std::string outPath = argv[2];

    // Collect key and file-path pairs
    struct AttrFile {
        std::string key;
        std::string file;
        std::vector<char> valueBytes; // For preloading values & sizes
    };
    std::vector<AttrFile> newAttrs;
    for (int i = 3; i + 1 < argc; i += 2) {
        AttrFile af;
        af.key = argv[i];
        af.file = argv[i+1];

        // Load value bytes from file
        std::ifstream fval(af.file, std::ios::binary);
        if (!fval) {
            std::cerr << "Error: cannot open value file: " << af.file << std::endl;
            return 1;
        }
        af.valueBytes = std::vector<char>(
            (std::istreambuf_iterator<char>(fval)),
            std::istreambuf_iterator<char>()
        );
        // --- MINIMAL FIX: Strip trailing null if present ---
        if (!af.valueBytes.empty() && af.valueBytes.back() == '\0')
            af.valueBytes.pop_back();

        newAttrs.push_back(std::move(af));
    }

    // --- PASS 1: Read Header & Stream-Copy the Rest ---

    // 1) Parse header + old attributes into headerBuf
    std::ifstream fin(inPath, std::ios::binary);
    if (!fin) { perror("open input"); return 1; }

    std::vector<char> headerBuf;
    headerBuf.reserve(1<<20); // ~1 MB buffer for header

    // helper to read one byte and push it
    auto readPush = [&](char &c) {
        fin.read(&c,1);
        if (!fin) { std::cerr<<"Unexpected EOF\n"; exit(1); }
        headerBuf.push_back(c);
    };

    // a) magic string (null-terminated)
    char c;
    do { readPush(c); } while(c!='\0');

    // b) version (int32)
    char tmp4[4];
    fin.read(tmp4,4); headerBuf.insert(headerBuf.end(), tmp4, tmp4+4);
    int32_t version = readInt32LE(tmp4);

    // c) footerPosition
    size_t footerPosField = headerBuf.size();
    char tmp8[8];
    fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
    int64_t origFooterPos = readInt64LE(tmp8);

    // d) genomeID
    do { readPush(c); } while(c!='\0');

    // e) normVectorIndexPosition & length (if v9+)
    size_t nviPosField = 0, nviLenField=0;
    int64_t origNviPos = 0;
    if (version > 8) {
        nviPosField = headerBuf.size();
        fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
        origNviPos = readInt64LE(tmp8);
        nviLenField = headerBuf.size();
        fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
    }

    // f) attribute count
    size_t attrCountField = headerBuf.size();
    fin.read(tmp4,4); headerBuf.insert(headerBuf.end(), tmp4, tmp4+4);
    int32_t origAttrCount = readInt32LE(tmp4);

    // g) read each existing key\0value\0
    for (int i = 0; i < origAttrCount; i++) {
        do { readPush(c); } while(c!='\0');  // key
        do { readPush(c); } while(c!='\0');  // value
    }
    size_t attrListStart = attrCountField + 4;
    size_t attrListEnd   = headerBuf.size();

    // Compute delta: new bytes added to header
    int32_t newAttrCount = origAttrCount + (int)newAttrs.size();
    size_t extraBytes = 0;
    for (const auto &af : newAttrs) {
        // key+\0 + value-bytes + \0
        extraBytes += af.key.size() + 1 + af.valueBytes.size() + 1;
    }
    size_t delta = extraBytes;

    // 2) Open output and write updated header
    std::ofstream fout(outPath, std::ios::binary);
    if (!fout) { perror("open output"); return 1; }

    // Copy up to attrCountField
    fout.write(headerBuf.data(), attrCountField);

    // Write updated count
    char countBuf[4];
    writeInt32LE(countBuf, newAttrCount);
    fout.write(countBuf,4);

    // Copy old attributes
    fout.write(headerBuf.data() + attrListStart, attrListEnd - attrListStart);

    // Append new attributes (with value file contents, null-terminated)
    for (const auto &af : newAttrs) {
        // Write key + null
        fout.write(af.key.c_str(), af.key.size());
        fout.put('\0');
        // Write value bytes from file, then null
        if (!af.valueBytes.empty())
            fout.write(af.valueBytes.data(), af.valueBytes.size());
        fout.put('\0'); // <-- This is crucial!
    }

    // 3) Stream-copy the rest of the file
    fin.seekg(attrListEnd, std::ios::beg);
    const size_t BUF_SZ = 1<<20; // 1 MiB
    std::vector<char> buf(BUF_SZ);
    while (fin) {
        fin.read(buf.data(), BUF_SZ);
        fout.write(buf.data(), fin.gcount());
    }
    fin.close();
    fout.close();

    // --- PASS 2: Patch All Pointers In-Place ---

    std::fstream fupd(outPath, std::ios::in|std::ios::out|std::ios::binary);
    if (!fupd) { perror("reopen output"); return 1; }

    // a) header pointers
    fupd.seekp(footerPosField, std::ios::beg);
    char p8[8];
    writeInt64LE(p8, origFooterPos + (int64_t)delta);
    fupd.write(p8,8);

    if (version > 8) {
        fupd.seekp(nviPosField, std::ios::beg);
        writeInt64LE(p8, origNviPos + (int64_t)delta);
        fupd.write(p8,8);
    }

    // b) master-index entries
    int64_t newFooterPos = origFooterPos + (int64_t)delta;
    fupd.seekg(newFooterPos, std::ios::beg);

    // skip nBytesV5
    if (version > 8) fupd.seekg(8, std::ios::cur);
    else             fupd.seekg(4, std::ios::cur);

    // read nEntries
    char i4[4];
    fupd.read(i4,4);
    int32_t nEntries = readInt32LE(i4);

    for (int32_t i = 0; i < nEntries; i++) {
        // skip key string
        char ch;
        do { fupd.get(ch); } while(ch!='\0');

        // now at 'position' field
        std::streamoff posField = fupd.tellg();
        fupd.read(p8,8);
        int64_t origPos = readInt64LE(p8);
        writeInt64LE(p8, origPos + (int64_t)delta);
        fupd.seekp(posField, std::ios::beg);
        fupd.write(p8,8);

        // skip size
        fupd.seekg(4, std::ios::cur);
    }

    // c) normalization-vector index
    if (version > 8) {
        int64_t newNviPos = origNviPos + (int64_t)delta;
        fupd.seekg(newNviPos, std::ios::beg);

        // read nNormVectors
        fupd.read(i4,4);
        int32_t nNorm = readInt32LE(i4);

        for (int i = 0; i < nNorm; i++) {
            // skip type\0, chrIdx, unit\0, binSize
            char ch;
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur);
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur);

            // patch the position field
            std::streamoff pp = fupd.tellg();
            fupd.read(p8,8);
            int64_t oP = readInt64LE(p8);
            writeInt64LE(p8, oP + (int64_t)delta);
            fupd.seekp(pp, std::ios::beg);
            fupd.write(p8,8);

            // skip nBytes
            fupd.seekg(8, std::ios::cur);
        }
    }

    fupd.close();
    std::cout << "Wrote " << outPath
              << " with " << newAttrs.size()
              << " new attribute(s), pointers bumped by "
              << delta << " bytes.\n";
    return 0;
}
