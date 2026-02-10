#' Translate an R Expression to a LanceDB Filter String
#'
#' Takes an R expression (as a language object) and converts it into a
#' LanceDB-compatible SQL filter string.
#'
#' @param expr An R language object (from `rlang::quo_get_expr()`).
#'
#' @return A character string containing the translated filter expression.
#'
#' @noRd
translate_expr <- function(expr) {
  if (is.null(expr)) {
    rlang::abort("NULL expression cannot be translated to a filter.")
  }

  # Atomic values: numeric, character, logical

if (is.numeric(expr)) {
    return(format(expr, scientific = FALSE))
  }

  if (is.character(expr)) {
    # String literal — escape single quotes
    escaped <- gsub("'", "''", expr)
    return(paste0("'", escaped, "'"))
  }

  if (is.logical(expr)) {
    return(if (isTRUE(expr)) "TRUE" else "FALSE")
  }

  # Symbol (column name) — allow dotted names like stats.strength
  if (is.symbol(expr)) {
    name <- as.character(expr)
    return(name)
  }

  # NULL literal
  if (identical(expr, NULL)) {
    return("NULL")
  }

  # Call expressions
  if (is.call(expr)) {
    fn <- expr[[1]]
    fn_name <- if (is.symbol(fn)) as.character(fn) else deparse(fn)

    # --- Parentheses ---
    if (fn_name == "(") {
      inner <- translate_expr(expr[[2]])
      return(paste0("(", inner, ")"))
    }

    # --- Unary NOT ---
    if (fn_name == "!") {
      inner <- translate_expr(expr[[2]])
      return(paste0("NOT (", inner, ")"))
    }

    # --- Unary minus ---
    if (fn_name == "-" && length(expr) == 2) {
      inner <- translate_expr(expr[[2]])
      return(paste0("-", inner))
    }

    # --- Binary operators ---
    binary_ops <- c(
      "==" = "=",
      "!=" = "!=",
      ">"  = ">",
      ">=" = ">=",
      "<"  = "<",
      "<=" = "<=",
      "&"  = "AND",
      "&&" = "AND",
      "|"  = "OR",
      "||" = "OR",
      "+"  = "+",
      "-"  = "-",
      "*"  = "*",
      "/"  = "/"
    )

    if (fn_name %in% names(binary_ops)) {
      lhs <- translate_expr(expr[[2]])
      rhs <- translate_expr(expr[[3]])
      sql_op <- binary_ops[[fn_name]]
      return(paste0(lhs, " ", sql_op, " ", rhs))
    }

    # --- %in% operator ---
    if (fn_name == "%in%") {
      lhs <- translate_expr(expr[[2]])
      rhs_expr <- expr[[3]]

      # RHS should be a vector — evaluate it
      rhs_values <- tryCatch(
        eval(rhs_expr, envir = parent.frame(3)),
        error = function(e) {
          rlang::abort(paste0(
            "Cannot evaluate RHS of %in%: ", deparse(rhs_expr),
            "\nError: ", conditionMessage(e)
          ))
        }
      )

      if (is.character(rhs_values)) {
        escaped <- vapply(rhs_values, function(v) {
          paste0("'", gsub("'", "''", v), "'")
        }, character(1))
        vals_str <- paste(escaped, collapse = ", ")
      } else if (is.numeric(rhs_values)) {
        vals_str <- paste(format(rhs_values, scientific = FALSE), collapse = ", ")
      } else {
        rlang::abort("RHS of %in% must be character or numeric.")
      }

      return(paste0(lhs, " IN (", vals_str, ")"))
    }

    # --- is.na() ---
    if (fn_name == "is.na") {
      inner <- translate_expr(expr[[2]])
      return(paste0(inner, " IS NULL"))
    }

    # --- !is.na() is handled by unary NOT + is.na ---

    # --- c() for vector construction (used in %in% RHS) ---
    if (fn_name == "c") {
      # This shouldn't appear at top level, but handle gracefully
      rlang::abort("c() is not valid as a standalone filter expression.")
    }

    # --- $ operator for nested field access (e.g., stats$strength) ---
    if (fn_name == "$") {
      lhs <- translate_expr(expr[[2]])
      rhs <- as.character(expr[[3]])
      return(paste0(lhs, ".", rhs))
    }

    # --- between (if we want to support it) ---
    if (fn_name == "between") {
      col <- translate_expr(expr[[2]])
      lo <- translate_expr(expr[[3]])
      hi <- translate_expr(expr[[4]])
      return(paste0(col, " BETWEEN ", lo, " AND ", hi))
    }

    # --- Unsupported function ---
    rlang::abort(paste0(
      "Unsupported function in filter expression: `", fn_name, "()`.\n",
      "Supported: comparison operators (==, !=, >, <, >=, <=), ",
      "logical operators (&, |, !), %in%, is.na(), between().\n",
      "Alternatively, pass a string filter: filter(.data, \"your_expression\")"
    ))
  }

  # Fallback
rlang::abort(paste0(
    "Cannot translate expression of type '", typeof(expr), "': ",
    deparse(expr)
  ))
}
