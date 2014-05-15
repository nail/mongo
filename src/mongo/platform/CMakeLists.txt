add_library(platform STATIC
  backtrace
  posix_fadvise
  random
  process_id
  strcasestr
  )
add_dependencies(platform generate_error_codes generate_action_types)
