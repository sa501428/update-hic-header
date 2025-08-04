// g++ -std=c++11 update_hic_header_stream.cpp -o update_hic_header_stream
// ./update_hic_header_stream input.hic output.hic statistics statistics.txt graphs graphs.txt

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

struct AttrKV {
    std::string key, value;
};

std::vector<char> load_value_file(const std::string& file) {
    std::ifstream fval(file, std::ios::binary);
    if (!fval) {
        std::cerr << "Error: cannot open value file: " << file << std::endl;
        exit(1);
    }
    std::vector<char> valueBytes(
        (std::istreambuf_iterator<char>(fval)),
        std::istreambuf_iterator<char>()
    );
    while (!valueBytes.empty() && valueBytes.back() == '\0')
        valueBytes.pop_back();
    return valueBytes;
}

int main(int argc, char** argv) {
    if (argc != 7) {
        std::cerr << "Usage: " << argv[0]
                  << " <in.hic> <out.hic> statistics <file1> graphs <file2>\n";
        std::cerr << "  (Only 'statistics' and 'graphs' can be inserted in order after 'software'.)\n";
        return 1;
    }
    const std::string inPath  = argv[1];
    const std::string outPath = argv[2];

    std::string statKey = argv[3], statFile = argv[4];
    std::string graphKey = argv[5], graphFile = argv[6];
    if (statKey != "statistics" || graphKey != "graphs") {
        std::cerr << "Only 'statistics' and 'graphs' can be appended.\n";
        return 1;
    }

    // Preload values for new attrs
    std::vector<char> statVal = load_value_file(statFile);
    std::vector<char> graphVal = load_value_file(graphFile);

    // --- PASS 1: Read Header & Original Attributes ---

    std::ifstream fin(inPath, std::ios::binary);
    if (!fin) { perror("open input"); return 1; }

    std::vector<char> headerBuf;
    headerBuf.reserve(1<<20);

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
    size_t nviPosField = 0;
    int64_t origNviPos = 0;
    if (version > 8) {
        nviPosField = headerBuf.size();
        fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
        origNviPos = readInt64LE(tmp8);
        fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
    }

    // f) attribute count
    size_t attrCountField = headerBuf.size();
    fin.read(tmp4,4); headerBuf.insert(headerBuf.end(), tmp4, tmp4+4);
    int32_t origAttrCount = readInt32LE(tmp4);

    // g) read each existing key\0value\0
    std::vector<AttrKV> origAttrs;
    for (int i = 0; i < origAttrCount; i++) {
        std::string key, value;
        while (true) { fin.read(&c,1); headerBuf.push_back(c); if (c=='\0') break; key += c; }
        while (true) { fin.read(&c,1); headerBuf.push_back(c); if (c=='\0') break; value += c; }
        origAttrs.push_back({key, value});
    }
    size_t attrListStart = attrCountField + 4;
    size_t attrListEnd   = headerBuf.size();

    // --- PASS 2: Build Updated Attribute List ---

    // Remove any existing statistics/graphs
    std::vector<AttrKV> newAttrs;
    newAttrs.reserve(origAttrs.size() + 2);
    int softwareIdx = -1;
    for (size_t i = 0; i < origAttrs.size(); ++i) {
        const std::string& k = origAttrs[i].key;
        if (k == "software") softwareIdx = (int)newAttrs.size();
        if (k != "statistics" && k != "graphs") newAttrs.push_back(origAttrs[i]);
    }
    if (softwareIdx == -1) {
        std::cerr << "Could not find 'software' attribute to insert after.\n";
        return 1;
    }

    // Insert statistics/graphs after 'software'
    auto it = newAttrs.begin() + (softwareIdx + 1);
    it = newAttrs.insert(it, {"statistics", std::string(statVal.data(), statVal.size())});
    it = newAttrs.insert(it+1, {"graphs", std::string(graphVal.data(), graphVal.size())});

    int32_t newAttrCount = newAttrs.size();

    // Compute extra bytes (total size difference)
    size_t origAttrBytes = 0, newAttrBytes = 0;
    for (const auto& a : origAttrs)
        origAttrBytes += a.key.size() + 1 + a.value.size() + 1;
    for (const auto& a : newAttrs)
        newAttrBytes += a.key.size() + 1 + a.value.size() + 1;
    size_t delta = newAttrBytes - origAttrBytes;

    // --- Write updated header ---

    std::ofstream fout(outPath, std::ios::binary);
    if (!fout) { perror("open output"); return 1; }
    fout.write(headerBuf.data(), attrCountField);

    char countBuf[4];
    writeInt32LE(countBuf, newAttrCount);
    fout.write(countBuf,4);

    for (const auto& a : newAttrs) {
        fout.write(a.key.c_str(), a.key.size());
        fout.put('\0');
        if (!a.value.empty())
            fout.write(a.value.c_str(), a.value.size());
        fout.put('\0');
    }

    // Stream-copy the rest of the file
    fin.seekg(attrListEnd, std::ios::beg);
    const size_t BUF_SZ = 1<<20;
    std::vector<char> buf(BUF_SZ);
    while (fin) {
        fin.read(buf.data(), BUF_SZ);
        fout.write(buf.data(), fin.gcount());
    }
    fin.close();
    fout.close();

    // --- PASS 3: Patch Pointers ---

    std::fstream fupd(outPath, std::ios::in|std::ios::out|std::ios::binary);
    if (!fupd) { perror("reopen output"); return 1; }

    // a) header pointers
    fupd.seekp(footerPosField, std::ios::beg);
    writeInt64LE(tmp8, origFooterPos + (int64_t)delta);
    fupd.write(tmp8,8);

    if (version > 8) {
        fupd.seekp(nviPosField, std::ios::beg);
        writeInt64LE(tmp8, origNviPos + (int64_t)delta);
        fupd.write(tmp8,8);
    }

    // b) master-index entries
    int64_t newFooterPos = origFooterPos + (int64_t)delta;
    fupd.seekg(newFooterPos, std::ios::beg);

    if (version > 8) fupd.seekg(8, std::ios::cur);
    else             fupd.seekg(4, std::ios::cur);

    fupd.read(tmp4,4);
    int32_t nEntries = readInt32LE(tmp4);

    for (int32_t i = 0; i < nEntries; i++) {
        char ch;
        do { fupd.get(ch); } while(ch!='\0');
        std::streamoff posField = fupd.tellg();
        fupd.read(tmp8,8);
        int64_t origPos = readInt64LE(tmp8);
        writeInt64LE(tmp8, origPos + (int64_t)delta);
        fupd.seekp(posField, std::ios::beg);
        fupd.write(tmp8,8);
        fupd.seekg(4, std::ios::cur);
    }

    // c) normalization-vector index
    if (version > 8) {
        int64_t newNviPos = origNviPos + (int64_t)delta;
        fupd.seekg(newNviPos, std::ios::beg);
        fupd.read(tmp4,4);
        int32_t nNorm = readInt32LE(tmp4);
        for (int i = 0; i < nNorm; i++) {
            char ch;
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur);
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur);
            std::streamoff pp = fupd.tellg();
            fupd.read(tmp8,8);
            int64_t oP = readInt64LE(tmp8);
            writeInt64LE(tmp8, oP + (int64_t)delta);
            fupd.seekp(pp, std::ios::beg);
            fupd.write(tmp8,8);
            fupd.seekg(8, std::ios::cur);
        }
    }

    fupd.close();
    std::cout << "Wrote " << outPath
              << " with statistics/graphs inserted after software, pointers bumped by "
              << delta << " bytes.\n";
    return 0;
}
