#define BCON_H_
#include <bson/bson.h>

// libbson 2.3 added bson_append_array_unsafe_begin and deprecated bson_append_array_begin
// (same signature and behaviour). Use whichever this libbson has, so the package builds
// warning-free on current libbson and still builds on 2.0-2.2 (e.g. Ubuntu 26.04's 2.2.2).
static inline bool _perfect_bson_append_array_begin(bson_t *bson, const char *key, int key_length, bson_t *child)
{
#if BSON_CHECK_VERSION(2, 3, 0)
	return bson_append_array_unsafe_begin(bson, key, key_length, child);
#else
	return bson_append_array_begin(bson, key, key_length, child);
#endif
}
