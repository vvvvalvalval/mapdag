(ns mapdag.runtime.jvm-eval)

;; IMPROVEMENT "won't throw" metadata on fn.

;; sources of inefficiency:
;; 1. (apply )
;; 2. megamorphic call sites
;; 3. navigating in maps
;; 4. catch clauses?

;; IDEA bit masks for representing ancestors set? ;; https://drumcoder.co.uk/blog/2010/jan/06/bitmasks-java/
;; Could be mighty useful when the input keys are known AOT.




