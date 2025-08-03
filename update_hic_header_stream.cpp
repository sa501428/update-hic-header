#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

// little-endian readers/writers
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

// load a small file fully into a string
static std::string slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        std::cerr << "Error: cannot open value-file \"" << path << "\"\n";
        std::exit(1);
    }
    std::string out;
    f.seekg(0, std::ios::end);
    out.resize((size_t)f.tellg());
    f.seekg(0, std::ios::beg);
    f.read(&out[0], out.size());
    return out;
}

int main(int argc, char** argv) {
    if (argc < 5 || ((argc - 3) % 2) != 0) {
        std::cerr << "Usage: " << argv[0]
                  << " <in.hic> <out.hic> <key1> <value1|@file1> [<key2> <value2|@file2> ...]\n";
        return 1;
    }
    const std::string inPath  = argv[1];
    const std::string outPath = argv[2];

    // 1) collect new attributes, reading file-values when prefixed with '@'
    std::vector<std::pair<std::string, std::string>> newAttrs;
    for (int i = 3; i + 1 < argc; i += 2) {
        std::string key   = argv[i];
        std::string valArg= argv[i+1];
        std::string value;
        if (!valArg.empty() && valArg[0]=='@') {
            // read file contents
            value = slurp(valArg.substr(1));
        } else {
            value = valArg;
        }
        newAttrs.emplace_back(std::move(key), std::move(value));
    }

    // --- PASS 1: READ HEADER & STREAM-COPY THE REST ---

    std::ifstream fin(inPath, std::ios::binary);
    if (!fin) { perror("open input"); return 1; }

    std::vector<char> headerBuf;
    headerBuf.reserve(1<<20);

    auto readPush = [&](char &c) {
        fin.read(&c,1);
        if (!fin) { std::cerr<<"Unexpected EOF\n"; std::exit(1); }
        headerBuf.push_back(c);
    };

    // a) magic (null-term)
    char c;
    do { readPush(c); } while(c!='\0');

    // b) version
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

    // e) normVectorIndexPosition & length
    size_t nviPosField=0, nviLenField=0;
    int64_t origNviPos=0;
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

    // g) existing attrs
    for (int i = 0; i < origAttrCount; i++) {
        do { readPush(c); } while(c!='\0');
        do { readPush(c); } while(c!='\0');
    }
    size_t attrListStart = attrCountField + 4;
    size_t attrListEnd   = headerBuf.size();

    // compute delta & new count
    int32_t newAttrCount = origAttrCount + (int)newAttrs.size();
    size_t extraBytes = 0;
    for (auto &kv : newAttrs)
        extraBytes += kv.first.size()+1 + kv.second.size()+1;
    size_t delta = extraBytes;

    // 2) write updated header + new attrs
    std::ofstream fout(outPath, std::ios::binary);
    if (!fout) { perror("open output"); return 1; }

    fout.write(headerBuf.data(), attrCountField);
    char buf4[4];
    writeInt32LE(buf4, newAttrCount);
    fout.write(buf4,4);
    fout.write(headerBuf.data()+attrListStart, attrListEnd-attrListStart);

    for (auto &kv : newAttrs) {
        fout.write(kv.first.c_str(), kv.first.size());
        fout.put('\0');
        fout.write(kv.second.data(), kv.second.size());
        fout.put('\0');
    }

    // 3) stream-copy remainder
    fin.seekg(attrListEnd, std::ios::beg);
    const size_t CHUNK = 1<<20;
    std::vector<char> ioBuf(CHUNK);
    while (fin) {
        fin.read(ioBuf.data(), CHUNK);
        fout.write(ioBuf.data(), fin.gcount());
    }
    fin.close();
    fout.close();

    // --- PASS 2: PATCH POINTERS IN-PLACE ---

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

    // b) master-index
    fupd.seekg(origFooterPos + (int64_t)delta, std::ios::beg);
    // skip nBytesV5
    fupd.seekg(version>8 ? 8 : 4, std::ios::cur);
    char i4[4];
    fupd.read(i4,4);
    int32_t nEntries = readInt32LE(i4);

    for (int32_t i = 0; i < nEntries; i++) {
        char ch;
        do { fupd.get(ch); } while(ch!='\0');
        auto posField = fupd.tellg();
        fupd.read(p8,8);
        int64_t op = readInt64LE(p8);
        writeInt64LE(p8, op + (int64_t)delta);
        fupd.seekp(posField, std::ios::beg);
        fupd.write(p8,8);
        fupd.seekg(4, std::ios::cur);
    }

    // c) norm-vector index
    if (version > 8) {
        fupd.seekg(origNviPos + (int64_t)delta, std::ios::beg);
        fupd.read(i4,4);
        int32_t nNorm = readInt32LE(i4);

        for (int i = 0; i < nNorm; i++) {
            char ch;
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur);
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur);

            auto pp = fupd.tellg();
            fupd.read(p8,8);
            int64_t oP = readInt64LE(p8);
            writeInt64LE(p8, oP + (int64_t)delta);
            fupd.seekp(pp, std::ios::beg);
            fupd.write(p8,8);

            fupd.seekg(8, std::ios::cur);
        }
    }

    fupd.close();
    std::cout << "Written " << outPath
              << " with " << newAttrs.size()
              << " new attribute(s) (file-sourced where ‘@’prefixed), pointers bumped by "
              << delta << " bytes.\n";
    return 0;
}
