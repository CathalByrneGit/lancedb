test_that("translate_expr handles simple comparisons", {
  expect_equal(translate_expr(quote(x > 3)), "x > 3")
  expect_equal(translate_expr(quote(x >= 10)), "x >= 10")
  expect_equal(translate_expr(quote(x < 5)), "x < 5")
  expect_equal(translate_expr(quote(x <= 0)), "x <= 0")
  expect_equal(translate_expr(quote(x == 1)), "x = 1")
  expect_equal(translate_expr(quote(x != 2)), "x != 2")
})

test_that("translate_expr handles string literals", {
  expect_equal(
    translate_expr(quote(name == "Alice")),
    "name = 'Alice'"
  )
})

test_that("translate_expr handles boolean operators", {
  expect_equal(
    translate_expr(quote(x > 3 & y < 10)),
    "x > 3 AND y < 10"
  )
  expect_equal(
    translate_expr(quote(x > 3 | y < 10)),
    "x > 3 OR y < 10"
  )
  expect_equal(
    translate_expr(quote(!active)),
    "NOT (active)"
  )
})

test_that("translate_expr handles parentheses", {
  expect_equal(
    translate_expr(quote((x > 3))),
    "(x > 3)"
  )
  expect_equal(
    translate_expr(quote((x > 3) & (y < 10))),
    "(x > 3) AND (y < 10)"
  )
})

test_that("translate_expr handles is.na", {
  expect_equal(
    translate_expr(quote(is.na(x))),
    "x IS NULL"
  )
})

test_that("translate_expr handles %in%", {
  vals <- c("a", "b", "c")
  expr <- bquote(name %in% .(vals))
  result <- translate_expr(expr)
  expect_equal(result, "name IN ('a', 'b', 'c')")
})

test_that("translate_expr handles numeric %in%", {
  vals <- c(1, 2, 3)
  expr <- bquote(id %in% .(vals))
  result <- translate_expr(expr)
  expect_equal(result, "id IN (1, 2, 3)")
})

test_that("translate_expr handles nested field access via $", {
  expect_equal(
    translate_expr(quote(stats$strength > 3)),
    "stats.strength > 3"
  )
})

test_that("translate_expr handles between", {
  expect_equal(
    translate_expr(quote(between(age, 18, 65))),
    "age BETWEEN 18 AND 65"
  )
})

test_that("translate_expr rejects unsupported functions", {
  expect_error(
    translate_expr(quote(sqrt(x))),
    "Unsupported function"
  )
  expect_error(
    translate_expr(quote(mean(x))),
    "Unsupported function"
  )
})

test_that("translate_expr handles bare column names", {
  expect_equal(translate_expr(quote(active)), "active")
})

test_that("translate_expr handles negative numbers", {
  expect_equal(translate_expr(quote(-5)), "-5")
  expect_equal(translate_expr(quote(x > -1)), "x > -1")
})
