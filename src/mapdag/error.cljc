(ns mapdag.error)

(def error-keys
  "The error keys (found in ex-data maps) that are part mapdag's public API.
  You may find other keys in Exception data, but you should not rely on them programmatically."
  #{:mapdag.error/reason ;; INTRO a keyword, describing the type of error. (Val, 20 May 2020)
    :mapdag.trace/deps-values

    :mapdag/step

    :mapdag.step/name
    :mapdag.step/deps})