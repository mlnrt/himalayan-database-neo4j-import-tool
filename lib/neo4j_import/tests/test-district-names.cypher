// Get all districts
MATCH (d:District)
WHERE d.name CONTAINS '/'
// Return the districts containing a "/" in there name (there should be none)
RETURN d
