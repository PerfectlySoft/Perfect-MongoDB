#include <mongoc.h>

static bool _mongoc_cursor_next(mongoc_cursor_t *cursor, const void **bson)
{
	const bson_t *bson2 = NULL;
	bool r = mongoc_cursor_next(cursor, &bson2);
	*bson = bson2;
	return r;
}