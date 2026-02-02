test_that("lancedbr_config() reads env vars with defaults", {
  withr::local_envvar(c(
    LANCEDBR_PYTHON = NA_character_,
    LANCEDBR_CONDA_ENV = NA_character_
  ))

  cfg <- lancedbr_config()
  expect_true(is.list(cfg))
  expect_true("python" %in% names(cfg))
  expect_true("conda_env" %in% names(cfg))

  expect_true(is.na(cfg$python))
  expect_true(is.na(cfg$conda_env))
})


test_that("lancedbr_config() respects env overrides", {
  withr::local_envvar(c(
    LANCEDBR_PYTHON = "/usr/bin/python3",
    LANCEDBR_CONDA_ENV = "myenv"
  ))

  cfg <- lancedbr_config()
  expect_identical(cfg$python, "/usr/bin/python3")
  expect_identical(cfg$conda_env, "myenv")
})
