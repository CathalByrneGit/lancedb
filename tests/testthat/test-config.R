test_that("lancedb_config() reads env vars with defaults", {
  withr::local_envvar(c(
    LANCEDB_PYTHON = NA_character_,
    LANCEDB_CONDA_ENV = NA_character_
  ))

  cfg <- lancedb_config()
  expect_true(is.list(cfg))
  expect_true("python" %in% names(cfg))
  expect_true("conda_env" %in% names(cfg))

  expect_true(is.na(cfg$python))
  expect_true(is.na(cfg$conda_env))
})


test_that("lancedb_config() respects env overrides", {
  withr::local_envvar(c(
    LANCEDB_PYTHON = "/usr/bin/python3",
    LANCEDB_CONDA_ENV = "myenv"
  ))

  cfg <- lancedb_config()
  expect_identical(cfg$python, "/usr/bin/python3")
  expect_identical(cfg$conda_env, "myenv")
})
