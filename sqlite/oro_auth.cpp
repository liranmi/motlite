#include "oro_auth.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>

#ifdef ORO_HAVE_OPENSSL
#include <openssl/evp.h>
#endif

namespace {

std::string toHex(const unsigned char* buf, size_t len) {
    static const char hex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; i++) {
        out.push_back(hex[buf[i] >> 4]);
        out.push_back(hex[buf[i] & 0x0F]);
    }
    return out;
}

}  // namespace

std::string oroAuthMd5Hex(const void* data, size_t len) {
#ifdef ORO_HAVE_OPENSSL
    unsigned char digest[16];
    unsigned int dlen = sizeof(digest);
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_md5(), nullptr);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, digest, &dlen);
    EVP_MD_CTX_free(ctx);
    return toHex(digest, 16);
#else
    (void)data; (void)len;
    return "";  // MD5 unavailable — auth disabled at higher level.
#endif
}

bool oroAuthLoadUsers(const char* path, OroUsers& out) {
    out.md5_hashes.clear();
    out.enabled = false;
    if (!path || !*path) return false;
    std::ifstream f(path);
    if (!f.is_open()) return false;

    std::string line;
    while (std::getline(f, line)) {
        // Trim leading whitespace.
        size_t s = line.find_first_not_of(" \t");
        if (s == std::string::npos) continue;
        if (line[s] == '#') continue;
        std::istringstream is(line.substr(s));
        std::string user, secret;
        if (!(is >> user >> secret)) continue;
        // PG-style md5<hex> means: secret is the md5(password+user) already.
        std::string hash;
        if (secret.size() == 35 && secret.compare(0, 3, "md5") == 0) {
            hash = secret.substr(3);
            for (auto& c : hash) if (c >= 'A' && c <= 'F') c = (char)(c + 32);
        } else {
            // Plaintext password — store md5(password + user).
            std::string pwUser = secret + user;
            hash = oroAuthMd5Hex(pwUser.data(), pwUser.size());
        }
        if (hash.size() == 32) out.md5_hashes[user] = hash;
    }
    out.enabled = !out.md5_hashes.empty();
    return out.enabled;
}

std::string oroAuthExpectedResponse(const std::string& userMd5,
                                    const uint8_t salt[4]) {
    if (userMd5.size() != 32) return "";
    // Build buffer: userMd5 (32 hex chars) + 4-byte salt.
    std::string buf;
    buf.reserve(36);
    buf.append(userMd5);
    buf.append(reinterpret_cast<const char*>(salt), 4);
    return "md5" + oroAuthMd5Hex(buf.data(), buf.size());
}
