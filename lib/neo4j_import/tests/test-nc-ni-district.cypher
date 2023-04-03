// Get all districts
MATCH (d:District)
WHERE d.name = "NC" OR d.name = "NI"
// Return the districts which are "NC" (China) or "NI" (India). There should be none
RETURN d
