// g++ -std=c++11 update_hic_header.cpp -o update_hic_header
// ./update_hic_header input.hic output.hic statistics statistics.txt graphs graphs.txt

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <map>

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

// Helper to mimic Juicer: read file as lines, append '\n' after each line.
std::vector<char> load_value_file_text(const std::string& file) {
    std::ifstream fval(file);
    if (!fval) {
        std::cerr << "Error: cannot open value file: " << file << std::endl;
        exit(1);
    }
    std::string line, value;
    while (std::getline(fval, line)) {
        value += line + "\n";
    }
    return std::vector<char>(value.begin(), value.end());
}

// Read null-terminated string from stream
std::string readNullTerminatedString(std::ifstream& fin) {
    std::string result;
    char c;
    while (fin.get(c) && c != '\0') {
        result += c;
    }
    return result;
}

// Write null-terminated string to stream
void writeNullTerminatedString(std::ofstream& fout, const std::string& str) {
    fout.write(str.c_str(), str.length());
    fout.put('\0');
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

    // Use Juicer-style text read for statistics/graphs
    std::vector<char> statVal = load_value_file_text(statFile);
    std::vector<char> graphVal = load_value_file_text(graphFile);

    // --- PASS 1: Read Header & Original Attributes ---

    std::ifstream fin(inPath, std::ios::binary);
    if (!fin) { 
        std::cerr << "Error: cannot open input file: " << inPath << std::endl;
        return 1; 
    }

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

    // c) footerPosition (master index position)
    size_t footerPosField = headerBuf.size();
    char tmp8[8];
    fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
    int64_t origFooterPos = readInt64LE(tmp8);

    // d) genomeID
    do { readPush(c); } while(c!='\0');

    // e) normVectorIndexPosition & length (if v9+)
    size_t nviPosField = 0;
    int64_t origNviPos = 0;
    int64_t origNviLen = 0;
    if (version > 8) {
        nviPosField = headerBuf.size();
        fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
        origNviPos = readInt64LE(tmp8);
        fin.read(tmp8,8); headerBuf.insert(headerBuf.end(), tmp8, tmp8+8);
        origNviLen = readInt64LE(tmp8);
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
    size_t attrListEnd = headerBuf.size();

    // h) Read chromosome dictionary
    std::vector<char> chrDictBuf;
    chrDictBuf.reserve(1<<16);
    
    // Number of chromosomes
    fin.read(tmp4,4); chrDictBuf.insert(chrDictBuf.end(), tmp4, tmp4+4);
    int32_t nChrs = readInt32LE(tmp4);
    
    // Read each chromosome entry
    for (int i = 0; i < nChrs; i++) {
        // Chromosome name (null-terminated)
        do { 
            fin.read(&c,1); 
            chrDictBuf.push_back(c); 
        } while(c!='\0');
        
        // Chromosome size (int32 for v8-, int64 for v9+)
        if (version > 8) {
            fin.read(tmp8,8); chrDictBuf.insert(chrDictBuf.end(), tmp8, tmp8+8);
        } else {
            fin.read(tmp4,4); chrDictBuf.insert(chrDictBuf.end(), tmp4, tmp4+4);
        }
    }
    
    // i) Read resolution arrays
    std::vector<char> resolutionBuf;
    resolutionBuf.reserve(1<<16);
    
    // BP resolutions
    fin.read(tmp4,4); resolutionBuf.insert(resolutionBuf.end(), tmp4, tmp4+4);
    int32_t nBpRes = readInt32LE(tmp4);
    for (int i = 0; i < nBpRes; i++) {
        fin.read(tmp4,4); resolutionBuf.insert(resolutionBuf.end(), tmp4, tmp4+4);
    }
    
    // Fragment resolutions
    fin.read(tmp4,4); resolutionBuf.insert(resolutionBuf.end(), tmp4, tmp4+4);
    int32_t nFragRes = readInt32LE(tmp4);
    for (int i = 0; i < nFragRes; i++) {
        fin.read(tmp4,4); resolutionBuf.insert(resolutionBuf.end(), tmp4, tmp4+4);
    }
    
    size_t dataStart = fin.tellg();

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
    if (!fout) { 
        std::cerr << "Error: cannot open output file: " << outPath << std::endl;
        return 1; 
    }
    
    // Write header up to attribute count
    fout.write(headerBuf.data(), attrCountField);

    // Write new attribute count
    char countBuf[4];
    writeInt32LE(countBuf, newAttrCount);
    fout.write(countBuf,4);

    // Write new attributes
    for (const auto& a : newAttrs) {
        fout.write(a.key.c_str(), a.key.size());
        fout.put('\0');
        if (!a.value.empty())
            fout.write(a.value.c_str(), a.value.size());
        fout.put('\0');
    }

    // Write chromosome dictionary
    fout.write(chrDictBuf.data(), chrDictBuf.size());

    // Write resolution arrays
    fout.write(resolutionBuf.data(), resolutionBuf.size());

    // Stream-copy the rest of the file
    fin.seekg(dataStart, std::ios::beg);
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
    if (!fupd) { 
        std::cerr << "Error: cannot reopen output file for pointer updates" << std::endl;
        return 1; 
    }

    // a) header pointers
    fupd.seekp(footerPosField, std::ios::beg);
    writeInt64LE(tmp8, origFooterPos + (int64_t)delta);
    fupd.write(tmp8,8);

    if (version > 8) {
        fupd.seekp(nviPosField, std::ios::beg);
        writeInt64LE(tmp8, origNviPos + (int64_t)delta);
        fupd.write(tmp8,8);
        fupd.seekp(nviPosField + 8, std::ios::beg);
        writeInt64LE(tmp8, origNviLen);
        fupd.write(tmp8,8);
    }

    // b) master-index entries
    int64_t newFooterPos = origFooterPos + (int64_t)delta;
    fupd.seekg(newFooterPos, std::ios::beg);

    // Read footer size
    long footerSize;
    if (version > 8) {
        fupd.read(tmp8,8);
        footerSize = readInt64LE(tmp8);
    } else {
        fupd.read(tmp4,4);
        footerSize = readInt32LE(tmp4);
    }

    // Read number of entries
    fupd.read(tmp4,4);
    int32_t nEntries = readInt32LE(tmp4);

    // Update each master index entry
    for (int32_t i = 0; i < nEntries; i++) {
        // Skip key string
        char ch;
        do { fupd.get(ch); } while(ch!='\0');
        
        // Update position
        std::streamoff posField = fupd.tellg();
        fupd.read(tmp8,8);
        int64_t origPos = readInt64LE(tmp8);
        writeInt64LE(tmp8, origPos + (int64_t)delta);
        fupd.seekp(posField, std::ios::beg);
        fupd.write(tmp8,8);
        fupd.seekg(4, std::ios::cur); // Skip size field
    }

    // c) normalization-vector index (if version > 8)
    if (version > 8) {
        int64_t newNviPos = origNviPos + (int64_t)delta;
        fupd.seekg(newNviPos, std::ios::beg);
        fupd.read(tmp4,4);
        int32_t nNorm = readInt32LE(tmp4);
        
        for (int i = 0; i < nNorm; i++) {
            // Skip type string
            char ch;
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur); // Skip chrIdx
            // Skip unit string
            do { fupd.get(ch); } while(ch!='\0');
            fupd.seekg(4, std::ios::cur); // Skip resolution
            
            // Update position
            std::streamoff pp = fupd.tellg();
            fupd.read(tmp8,8);
            int64_t oP = readInt64LE(tmp8);
            writeInt64LE(tmp8, oP + (int64_t)delta);
            fupd.seekp(pp, std::ios::beg);
            fupd.write(tmp8,8);
            fupd.seekg(8, std::ios::cur); // Skip sizeInBytes
        }
    }

    fupd.close();
    std::cout << "Successfully wrote " << outPath
              << " with statistics/graphs inserted after software, pointers bumped by "
              << delta << " bytes.\n";
    return 0;
} 