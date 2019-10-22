# Used by "mix format"
[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: [plug: :*, session: 1, table: 2],
  line_length: 120,
  export: [
    locals_without_parens: [session: 1, table: 2]
  ]
]
