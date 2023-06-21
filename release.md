Added

- divide duration 48+ in (48-72, 72-96, 96+)
- split 0-24 duration slot in 0-8 and 8-24 duration slots
- demand query bug resolved: all demand were going in 0-24 cohort
- supply query bug resolved: Car ids are being missed in supply query when there is no entry in car block table
- publish dpm of all the active cars irrespective of their car movements and car blocks
- only demand taken for dpm calculation instead of both supply and demand
