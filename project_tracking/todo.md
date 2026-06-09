Short term:
- [ ] set up postgres database
- [ ] add wnba and g league as leagues
- [ ] move season format to sources, ensure does not conflict with league season format naming conventions
- [ ] add roster_maintainer and season_detector datasets (do we need roles for datasets?)
- [ ] set up per_poss table and per_min table with domain normalization and rating
- [ ] set up staging/mapping to core tables
- [ ] set up matching

Long term:
- [ ] redesign sheet layout for better visibility and new use-cases (more number context, more condensed, multi-team stats)
- [ ] add player view
- [ ] rewrite publish to be source-agnostic, consistent, and config-driven
- [ ] Do I want game_by_game stats or just by season? would standard deviation/consistency be valuable? would all my   data be consistently available?
- [ ] Find a RAPM source
- [ ] Find a contracts source
- [ ] Find an injuries source
- [ ] set up db column comments
- [ ] rewrite all comments/documentation