#include <mongoc/mongoc.h>
#include <string.h>

static bool _mongoc_cursor_next(mongoc_cursor_t *cursor, const void **bson)
{
	const bson_t *bson2 = NULL;
	bool r = mongoc_cursor_next(cursor, &bson2);
	*bson = bson2;
	return r;
}

// libmongoc 2.x removed the legacy query APIs (mongoc_collection_find, _count, _save).
// These helpers keep Perfect-MongoDB's legacy-style parameters working on top of the
// current *_with_opts APIs, following what libmongoc 1.x did internally.

// Split a legacy query document into a filter and find options.
// { $query: {...}, $orderby: {...}, $hint: ... } becomes filter {...} and opts { sort: {...}, hint: ... }.
// A query without $query is copied into filter unchanged.
static void _perfect_split_legacy_query(const bson_t *query, bson_t *filter, bson_t *opts)
{
	bson_iter_t iter;
	if (!query || !bson_iter_init_find(&iter, query, "$query")) {
		if (query) {
			bson_concat(filter, query);
		}
		return;
	}
	bson_iter_init(&iter, query);
	while (bson_iter_next(&iter)) {
		const char *key = bson_iter_key(&iter);
		if (strcmp(key, "$query") == 0) {
			if (BSON_ITER_HOLDS_DOCUMENT(&iter)) {
				uint32_t len = 0;
				const uint8_t *data = NULL;
				bson_t sub;
				bson_iter_document(&iter, &len, &data);
				if (bson_init_static(&sub, data, len)) {
					bson_concat(filter, &sub);
				}
			}
		} else if (strcmp(key, "$orderby") == 0) {
			bson_append_iter(opts, "sort", -1, &iter);
		} else if (strcmp(key, "$showDiskLoc") == 0) {
			bson_append_iter(opts, "showRecordId", -1, &iter);
		} else if (strcmp(key, "$explain") == 0 || strcmp(key, "$snapshot") == 0) {
			// no longer supported by the server
		} else if (key[0] == '$') {
			bson_append_iter(opts, key + 1, -1, &iter);
		} else {
			bson_append_iter(filter, key, -1, &iter);
		}
	}
}

// Convert legacy query flags to find options. Returns true when MONGOC_QUERY_SECONDARY_OK is set.
static bool _perfect_flags_to_opts(mongoc_query_flags_t flags, bson_t *opts)
{
	if (flags & MONGOC_QUERY_TAILABLE_CURSOR) {
		BSON_APPEND_BOOL(opts, "tailable", true);
	}
	if (flags & MONGOC_QUERY_OPLOG_REPLAY) {
		BSON_APPEND_BOOL(opts, "oplogReplay", true);
	}
	if (flags & MONGOC_QUERY_NO_CURSOR_TIMEOUT) {
		BSON_APPEND_BOOL(opts, "noCursorTimeout", true);
	}
	if (flags & MONGOC_QUERY_AWAIT_DATA) {
		BSON_APPEND_BOOL(opts, "awaitData", true);
	}
	if (flags & MONGOC_QUERY_EXHAUST) {
		BSON_APPEND_BOOL(opts, "exhaust", true);
	}
	if (flags & MONGOC_QUERY_PARTIAL) {
		BSON_APPEND_BOOL(opts, "allowPartialResults", true);
	}
	return (flags & MONGOC_QUERY_SECONDARY_OK) != 0;
}

static mongoc_cursor_t *_perfect_collection_find(mongoc_collection_t *collection,
												 mongoc_query_flags_t flags,
												 uint32_t skip,
												 int32_t limit,
												 uint32_t batch_size,
												 const bson_t *query,
												 const bson_t *fields)
{
	bson_t filter = BSON_INITIALIZER;
	bson_t opts = BSON_INITIALIZER;
	mongoc_read_prefs_t *prefs = NULL;
	mongoc_cursor_t *cursor;

	_perfect_split_legacy_query(query, &filter, &opts);
	if (_perfect_flags_to_opts(flags, &opts)) {
		prefs = mongoc_read_prefs_new(MONGOC_READ_SECONDARY_PREFERRED);
	}
	if (skip) {
		BSON_APPEND_INT64(&opts, "skip", skip);
	}
	if (limit) {
		BSON_APPEND_INT64(&opts, "limit", limit);
	}
	if (batch_size) {
		BSON_APPEND_INT64(&opts, "batchSize", batch_size);
	}
	if (fields && !bson_empty(fields)) {
		BSON_APPEND_DOCUMENT(&opts, "projection", fields);
	}
	cursor = mongoc_collection_find_with_opts(collection, &filter, &opts, prefs);
	if (prefs) {
		mongoc_read_prefs_destroy(prefs);
	}
	bson_destroy(&filter);
	bson_destroy(&opts);
	return cursor;
}

static int64_t _perfect_collection_count(mongoc_collection_t *collection,
										 mongoc_query_flags_t flags,
										 const bson_t *query,
										 int64_t skip,
										 int64_t limit,
										 bson_error_t *error)
{
	bson_t filter = BSON_INITIALIZER;
	bson_t find_opts = BSON_INITIALIZER;
	bson_t opts = BSON_INITIALIZER;
	mongoc_read_prefs_t *prefs = NULL;
	int64_t count;

	_perfect_split_legacy_query(query, &filter, &find_opts);
	if (_perfect_flags_to_opts(flags, &find_opts)) {
		prefs = mongoc_read_prefs_new(MONGOC_READ_SECONDARY_PREFERRED);
	}
	if (skip) {
		BSON_APPEND_INT64(&opts, "skip", skip);
	}
	if (limit) {
		BSON_APPEND_INT64(&opts, "limit", limit);
	}
	count = mongoc_collection_count_documents(collection, &filter, &opts, prefs, NULL, error);
	if (prefs) {
		mongoc_read_prefs_destroy(prefs);
	}
	bson_destroy(&filter);
	bson_destroy(&find_opts);
	bson_destroy(&opts);
	return count;
}

static mongoc_gridfs_file_list_t *_perfect_gridfs_find(mongoc_gridfs_t *gridfs, const bson_t *query)
{
	bson_t filter = BSON_INITIALIZER;
	bson_t opts = BSON_INITIALIZER;
	mongoc_gridfs_file_list_t *list;

	_perfect_split_legacy_query(query, &filter, &opts);
	list = mongoc_gridfs_find_with_opts(gridfs, &filter, &opts);
	bson_destroy(&filter);
	bson_destroy(&opts);
	return list;
}

// True when the first key starts with '$', i.e. an update-operator document like { $set: ... }
// rather than a replacement document.
static bool _perfect_is_update_document(const bson_t *update)
{
	bson_iter_t iter;
	return bson_iter_init(&iter, update) && bson_iter_next(&iter) && bson_iter_key(&iter)[0] == '$';
}

static bool _perfect_collection_insert(mongoc_collection_t *collection,
									   mongoc_insert_flags_t flags,
									   const bson_t *document,
									   bson_error_t *error)
{
	bson_t opts = BSON_INITIALIZER;
	bool ret;

	if (flags & MONGOC_INSERT_NO_VALIDATE) {
		BSON_APPEND_BOOL(&opts, "validate", false);
	}
	ret = mongoc_collection_insert_one(collection, document, &opts, NULL, error);
	bson_destroy(&opts);
	return ret;
}

// Legacy update accepted either an update-operator document or a replacement document.
static bool _perfect_collection_update(mongoc_collection_t *collection,
									   mongoc_update_flags_t flags,
									   const bson_t *selector,
									   const bson_t *update,
									   bson_error_t *error)
{
	bson_t opts = BSON_INITIALIZER;
	bool ret;

	if (flags & MONGOC_UPDATE_UPSERT) {
		BSON_APPEND_BOOL(&opts, "upsert", true);
	}
	if (flags & MONGOC_UPDATE_NO_VALIDATE) {
		BSON_APPEND_BOOL(&opts, "validate", false);
	}
	if (!_perfect_is_update_document(update)) {
		ret = mongoc_collection_replace_one(collection, selector, update, &opts, NULL, error);
	} else if (flags & MONGOC_UPDATE_MULTI_UPDATE) {
		ret = mongoc_collection_update_many(collection, selector, update, &opts, NULL, error);
	} else {
		ret = mongoc_collection_update_one(collection, selector, update, &opts, NULL, error);
	}
	bson_destroy(&opts);
	return ret;
}

static bool _perfect_collection_remove(mongoc_collection_t *collection,
									   mongoc_remove_flags_t flags,
									   const bson_t *selector,
									   bson_error_t *error)
{
	if (flags & MONGOC_REMOVE_SINGLE_REMOVE) {
		return mongoc_collection_delete_one(collection, selector, NULL, NULL, error);
	}
	return mongoc_collection_delete_many(collection, selector, NULL, NULL, error);
}

// Legacy bulk update: updates every matching document, or replaces one for a replacement document.
static bool _perfect_bulk_operation_update(mongoc_bulk_operation_t *bulk,
										   const bson_t *selector,
										   const bson_t *update,
										   bson_error_t *error)
{
	if (!_perfect_is_update_document(update)) {
		return mongoc_bulk_operation_replace_one_with_opts(bulk, selector, update, NULL, error);
	}
	return mongoc_bulk_operation_update_many_with_opts(bulk, selector, update, NULL, error);
}

static bool _perfect_collection_find_and_modify(mongoc_collection_t *collection,
												const bson_t *query,
												const bson_t *sort,
												const bson_t *update,
												const bson_t *fields,
												bool _remove,
												bool upsert,
												bool _new,
												bson_t *reply,
												bson_error_t *error)
{
	bson_t empty = BSON_INITIALIZER;
	mongoc_find_and_modify_opts_t *opts = mongoc_find_and_modify_opts_new();
	int flags = MONGOC_FIND_AND_MODIFY_NONE;
	bool ret;

	if (sort) {
		mongoc_find_and_modify_opts_set_sort(opts, sort);
	}
	if (update) {
		mongoc_find_and_modify_opts_set_update(opts, update);
	}
	if (fields) {
		mongoc_find_and_modify_opts_set_fields(opts, fields);
	}
	if (_remove) {
		flags |= MONGOC_FIND_AND_MODIFY_REMOVE;
	}
	if (upsert) {
		flags |= MONGOC_FIND_AND_MODIFY_UPSERT;
	}
	if (_new) {
		flags |= MONGOC_FIND_AND_MODIFY_RETURN_NEW;
	}
	mongoc_find_and_modify_opts_set_flags(opts, (mongoc_find_and_modify_flags_t)flags);
	ret = mongoc_collection_find_and_modify_with_opts(collection, query ? query : &empty, opts, reply, error);
	mongoc_find_and_modify_opts_destroy(opts);
	bson_destroy(&empty);
	return ret;
}

// Legacy save: insert when the document has no _id, otherwise replace (upserting) by _id.
static bool _perfect_collection_save(mongoc_collection_t *collection, const bson_t *document, bson_error_t *error)
{
	bson_iter_t iter;
	bson_t selector = BSON_INITIALIZER;
	bson_t opts = BSON_INITIALIZER;
	bool ret;

	if (!bson_iter_init_find(&iter, document, "_id")) {
		return mongoc_collection_insert_one(collection, document, NULL, NULL, error);
	}
	bson_append_iter(&selector, "_id", 3, &iter);
	BSON_APPEND_BOOL(&opts, "upsert", true);
	ret = mongoc_collection_replace_one(collection, &selector, document, &opts, NULL, error);
	bson_destroy(&selector);
	bson_destroy(&opts);
	return ret;
}
