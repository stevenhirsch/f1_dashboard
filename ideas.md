This markdown file contains ideas for the F1 strategy dashboard.

- I like the general idea behind the streamlit app in the github repo, however I want to create a marimo app instead
- I want to have a top part of the app where I get the race summary for the grid, with the same search parameters
  - Instead of their tyre figure, I want to use my better, more detailed one.
- I also want to include the results for the entire race somewhere in this section (maybe separate of the tyres plot so it's more explicit)
- I want to also have a separate driver's section where I dive deeper into a specific driver on the grid
  - I then want to be able to dig deeper into each lap
    - Laps in the API has a date_start where we can get the general time that a lap starts, and lap duration can tell us when it ends
    - We can then associated car data telemetry & location data simultaneously
      - With this joint data, I'd want to have a colour-coded lap map where we use shades of green for the throttle and red for the brake.
        - I want the current lap throttle & brake, as well as the average at each location for the entire race
      - I want to know which tyres they are on, and how many laps they are on those tyres.
      - We can also show the lap times for each section for that lap, as well as static values for the average, best, and worst laps of the race
      - I want additional information if it was a pit-out lap, as well as how many laps since the last pit stop, and current position, and whether there is currently any flag (and which one)
        - Note that this is only relevant for races- for practices & qualifying this may not be relevant
    - Including the weather would also be nice (I think it's just overall weather, but specific weather for each lap is great)
