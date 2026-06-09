# Normalization Rules

1. Trim whitespace before and after text completely; in between text, trim whitespace to one space
2. Unicode NFC normalization (e + ́ -> e, etc)
3. Convert diacritics (ß -> ss, é -> e, ñ -> n, ö -> o, etc; we may need a library for this)
4. Normalize apostrophes to ', and hyphens to -
5. if standalone text, normalize 'Saint' to 'St', 'Sainte' to 'Ste', and 'Mount' to 'Mt'
6. Remove all periods and commas

(May need mapping too... University, University of, College, College of, State, Technical, etc handling...?)