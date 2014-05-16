add_library(logger STATIC
  console
  logger
  log_manager
  log_severity
  logstream_builder
  message_event_utf8_encoder
  message_log_domain
  ramlog
  rotatable_file_manager
  rotatable_file_writer
  )
add_dependencies(logger generate_error_codes generate_action_types)
