// g++ -std=c++11 update_hic_header_stream.cpp -o update_hic_header_stream
// ./update_hic_header_stream input.hic output.hic software "MyTool v1.2" graphs "chr1–chr2 stats"

// update_hic_header_stream.cpp
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

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
                  << " <in.hic> <out.hic> <key1> <value1> [<key2> <value2> ...]\n";
        return 1;
    }
    const std::string inPath  = argv[1];
    const std::string outPath = argv[2];

    // collect new attrs
    std::vector<std::pair<std::string,std::string>> newAttrs;
    for (int i = 3; i+1 < argc; i += 2)
        newAttrs.emplace_back(argv[i], argv[i+1]);

    // --- Attribute order fix: make sure statistics comes before hicFileScalingFactor ---
    {
        std::vector<std::pair<std::string,std::string>> orderedAttrs;
        // Add statistics first (if present)
        for (const auto& kv : newAttrs)
            if (kv.first == "statistics")
                orderedAttrs.push_back(kv);
        // Then hicFileScalingFactor (if present)
        for (const auto& kv : newAttrs)
            if (kv.first == "hicFileScalingFactor")
                orderedAttrs.push_back(kv);
        // Then any other attributes
        for (const auto& kv : newAttrs)
            if (kv.first != "statistics" && kv.first != "hicFileScalingFactor")
                orderedAttrs.push_back(kv);
        newAttrs = orderedAttrs;
    }

    // --- PASS 1: READ HEADER & STREAM-COPY THE REST ---

    // 1) parse header+old attributes into headerBuf
    std::ifstream fin(inPath, std::ios::binary);
    if (!fin) { perror("open input"); return 1; }

    std::vector<char> headerBuf;
    headerBuf.reserve(1<<20); // ~1 MB, should cover the whole header

    // helper to read one byte and push it
    auto readPush = [&](char &c) {
        fin.read(&c,1);
        if (!fin) { std::cerr<<"Unexpected EOF\n"; exit(1); }
        headerBuf.push_back(c);
    };

    // a) magic string (null‑terminated)
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
        // skip nviLength
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

    // compute delta
    int32_t newAttrCount = origAttrCount + (int)newAttrs.size();
    size_t extraBytes = 0;
    for (auto &kv : newAttrs)
        extraBytes += kv.first.size()+1 + kv.second.size()+1;
    size_t delta = extraBytes;

    // 2) open output and write updated header
    std::ofstream fout(outPath, std::ios::binary);
    if (!fout) { perror("open output"); return 1; }

    // copy up to attrCountField
    fout.write(headerBuf.data(), attrCountField);

    // write updated count
    char countBuf[4];
    writeInt32LE(countBuf, newAttrCount);
    fout.write(countBuf,4);

    // copy old attributes
    fout.write(headerBuf.data()+attrListStart,
               attrListEnd-attrListStart);

    // append new attributes
    for (auto &kv : newAttrs) {
        fout.write(kv.first.c_str(), kv.first.size());
        fout.put('\0');
        fout.write(kv.second.c_str(), kv.second.size());
        fout.put('\0');
    }

    // 3) stream‐copy the rest of the file
    fin.seekg(attrListEnd, std::ios::beg);
    const size_t BUF_SZ = 1<<20;               // 1 MiB buffer
    std::vector<char> buf(BUF_SZ);
    while (fin) {
        fin.read(buf.data(), BUF_SZ);
        fout.write(buf.data(), fin.gcount());
    }
    fin.close();
    fout.close();

    // --- PASS 2: PATCH ALL POINTERS IN-PLACE ---

    std::fstream fupd(outPath,
                      std::ios::in|std::ios::out|std::ios::binary);
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

    // b) master‐index entries
    int64_t newFooterPos = origFooterPos + (int64_t)delta;
    fupd.seekg(newFooterPos, std::ios::beg);

    // skip nBytesV5
    if (version > 8) fupd.seekg(8, std::ios::cur);
    else            fupd.seekg(4, std::ios::cur);

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

    // c) normalization‐vector index
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
