test_that("lancedb_lazy objects are created correctly by lancedb_search", {
  # Create a mock table object
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  qvec <- runif(4)
  lazy <- lancedb_search(mock_table, qvec)

  expect_s3_class(lazy, "lancedb_lazy")
  expect_equal(lazy$mode, "search")
  expect_equal(lazy$qvec, as.double(qvec))
  expect_equal(length(lazy$ops), 0)
  expect_equal(lazy$table$name, "test_table")
})

test_that("lancedb_lazy objects are created correctly by lancedb_scan", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)

  expect_s3_class(lazy, "lancedb_lazy")
  expect_equal(lazy$mode, "scan")
  expect_null(lazy$qvec)
  expect_equal(length(lazy$ops), 0)
})

test_that("verbs return new objects (immutability)", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy1 <- lancedb_scan(mock_table)
  lazy2 <- filter.lancedb_lazy(lazy1, "x > 3")
  lazy3 <- select.lancedb_lazy(lazy2, name, age)
  lazy4 <- slice_head.lancedb_lazy(lazy3, n = 10)

  # Original objects should be unchanged
expect_equal(length(lazy1$ops), 0)
  expect_equal(length(lazy2$ops), 1)
  expect_equal(length(lazy3$ops), 2)
  expect_equal(length(lazy4$ops), 3)

  # They should not be the same object
  expect_false(identical(lazy1, lazy2))
  expect_false(identical(lazy2, lazy3))
  expect_false(identical(lazy3, lazy4))
})

test_that("filter appends where ops correctly", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)

  # String filter
  lazy2 <- filter.lancedb_lazy(lazy, "age > 30")
  expect_equal(lazy2$ops[[1]]$op, "where")
  expect_equal(lazy2$ops[[1]]$expr, "age > 30")

  # R expression filter
  lazy3 <- filter.lancedb_lazy(lazy, x > 5)
  expect_equal(lazy3$ops[[1]]$op, "where")
  expect_equal(lazy3$ops[[1]]$expr, "x > 5")

  # Multiple predicates ANDed
  lazy4 <- filter.lancedb_lazy(lazy, x > 5, y == "hello")
  expect_equal(lazy4$ops[[1]]$op, "where")
  expect_match(lazy4$ops[[1]]$expr, "AND")
})

test_that("select appends select ops correctly", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)
  lazy2 <- select.lancedb_lazy(lazy, name, age, score)

  expect_equal(lazy2$ops[[1]]$op, "select")
  expect_equal(lazy2$ops[[1]]$cols, c("name", "age", "score"))
})

test_that("slice_head appends limit ops correctly", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)
  lazy2 <- slice_head.lancedb_lazy(lazy, n = 42)

  expect_equal(lazy2$ops[[1]]$op, "limit")
  expect_equal(lazy2$ops[[1]]$n, 42L)
})

test_that("slice_head rejects prop and by arguments", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)
  expect_error(slice_head.lancedb_lazy(lazy, n = 10, prop = 0.5), "prop")
  expect_error(slice_head.lancedb_lazy(lazy, n = 10, by = "group"), "by")
})

test_that("head works as an alias for limit", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)
  lazy2 <- head(lazy, 20)

  expect_equal(lazy2$ops[[1]]$op, "limit")
  expect_equal(lazy2$ops[[1]]$n, 20L)
})

test_that("ops_to_json serializes correctly", {
  ops <- list(
    list(op = "where", expr = "age > 30"),
    list(op = "select", cols = c("name", "age")),
    list(op = "limit", n = 10L)
  )

  json <- ops_to_json(ops)
  expect_type(json, "character")
  expect_match(json, '"op":"where"')
  expect_match(json, '"op":"select"')
  expect_match(json, '"op":"limit"')
  expect_match(json, '"n":10')
})

test_that("ops_to_json returns empty array for no ops", {
  expect_equal(ops_to_json(list()), "[]")
})

test_that("print.lancedb_lazy works without error", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)
  lazy2 <- filter.lancedb_lazy(lazy, "x > 3")
  lazy2 <- select.lancedb_lazy(lazy2, name, age)
  lazy2 <- slice_head.lancedb_lazy(lazy2, n = 5)

  expect_output(print(lazy2), "lancedb_lazy")
  expect_output(print(lazy2), "filter:")
  expect_output(print(lazy2), "select:")
  expect_output(print(lazy2), "limit:")
})

test_that("show_query works without error", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)
  lazy <- filter.lancedb_lazy(lazy, "age > 30")
  lazy <- select.lancedb_lazy(lazy, name, age)

  expect_output(show_query(lazy), "LanceDB Query Plan")
  expect_output(show_query(lazy), "WHERE")
  expect_output(show_query(lazy), "SELECT")
})

test_that("arrange builds order_by ops correctly", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  lazy <- lancedb_scan(mock_table)

  expect_message(
    lazy2 <- arrange.lancedb_lazy(lazy, age, desc(name)),
    "order_by"
  )

  expect_equal(lazy2$ops[[1]]$op, "order_by")
  expect_equal(lazy2$ops[[1]]$cols, c("age", "name"))
  expect_equal(lazy2$ops[[1]]$desc, c(FALSE, TRUE))
})

test_that("chaining multiple operations works correctly", {
  mock_table <- structure(
    list(ptr = NULL, name = "test_table", connection = NULL),
    class = "lancedb_table"
  )

  # Simulate a full pipeline (without collect, since that needs Rust)
  lazy <- lancedb_search(mock_table, runif(4))
  lazy <- filter.lancedb_lazy(lazy, "strength > 3")
  lazy <- select.lancedb_lazy(lazy, name, role, description)
  lazy <- slice_head.lancedb_lazy(lazy, n = 5)

  expect_equal(length(lazy$ops), 3)
  expect_equal(lazy$ops[[1]]$op, "where")
  expect_equal(lazy$ops[[2]]$op, "select")
  expect_equal(lazy$ops[[3]]$op, "limit")
  expect_equal(lazy$mode, "search")
  expect_length(lazy$qvec, 4)
})
