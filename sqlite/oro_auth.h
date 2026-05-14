/*
 * oro_auth.h — minimal MD5 password authentication for oro_server.
 *
 * Implements PostgreSQL's md5 auth flow:
 *   server → AuthenticationMD5Password (R, payload: int32(5) + 4-byte salt)
 *   client → PasswordMessage (p, "md5" + md5(md5(password + username) + salt))
 *   server → AuthenticationOk on match, ErrorResponse otherwise.
 *
 * Users file format (one entry per line, '#' = comment):
 *
 *   username password
 *
 * The "password" field can be:
 *   - plaintext (recommended for localhost dev only)
 *   - md5<hex> with hex = md5(password + username)  (PG-compatible format)
 *
 * When no users file is provided (--users not set, or load fails), the
 * server runs in trust mode (sends AuthenticationOk unconditionally) so
 * existing single-user setups keep working.
 */

#ifndef ORO_AUTH_H
#define ORO_AUTH_H

#include <stdint.h>
#include <string>
#include <unordered_map>

struct OroUsers {
    // user -> md5(password + username), lower-case hex.
    // Empty hash means user not found.
    std::unordered_map<std::string, std::string> md5_hashes;
    bool enabled = false;  // false => trust mode
};

// Load a users file. Returns true on success (file present and parsed),
// false otherwise (trust mode). On true return, `out.enabled` is set.
bool oroAuthLoadUsers(const char* path, OroUsers& out);

// Compute md5(md5(password+user) + salt) — the PG md5 client response.
// `salt` must be exactly 4 bytes. Returns lowercase hex.
std::string oroAuthExpectedResponse(const std::string& userMd5,
                                    const uint8_t salt[4]);

// Compute lowercase md5 hex of an arbitrary buffer.
std::string oroAuthMd5Hex(const void* data, size_t len);

#endif
