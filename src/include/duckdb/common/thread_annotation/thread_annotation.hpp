//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread_annotation/thread_annotation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

// CAPABILITY is an attribute on classes, which specifies that objects of the
// class can be used as a capability. The string argument specifies the kind of
// capability in error messages, e.g. "mutex".
#define DUCKDB_CAPABILITY(x) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(capability(x))

// SCOPED_CAPABILITY is an attribute on classes that implement RAII-style
// locking, in which a capability is acquired in the constructor, and released
// in the destructor. Such classes require special handling because the
// constructor and destructor refer to the capability via different names.
#define DUCKDB_SCOPED_CAPABILITY DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(scoped_lockable)
