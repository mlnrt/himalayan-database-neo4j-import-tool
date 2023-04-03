// Get all countries
MATCH (c:Country)
WHERE c.name CONTAINS '/'
// Return the countries containing a "/" in there name (there should be none)
RETURN c
